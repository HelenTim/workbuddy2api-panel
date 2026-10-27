// config.go 面板配置页接口：读取当前配置、校验并保存（热生效 + 重启项标注）。
//
// 分工：cmd/server 持有 Config 类型与校验逻辑（Load/normalize），此处只做
// HTTP 编排——GET 回显、POST 透传给注入的 SaveConfig 闭包（由 main 完成
// "校验 → 落盘 → 热应用 → 返回需重启字段列表"）。
package panel

import (
	"io"
	"log"
	"net/http"
	"os"
)

// getConfig 返回当前配置文件内容与路径（前端按 schema 渲染表单）。
func (p *Panel) getConfig(w http.ResponseWriter, r *http.Request) {
	if p.cfg.LoadConfig == nil {
		writeErr(w, http.StatusNotImplemented, "config api not available")
		return
	}
	cfg, err := p.cfg.LoadConfig()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "load config: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                         true,
		"path":                       p.cfg.ConfigPath,
		"config":                     cfg,
		"api_key_env_managed":        os.Getenv("WB2A_API_KEY") != "",
		"environment_managed_fields": configManagedFields(cfg),
		"r2_enabled":                 p.cfg.R2Enabled != nil && p.cfg.R2Enabled(),
	})
}

// saveConfig 保存配置：body 直接是配置 JSON（前端按 schema 组装完整对象）。
// SaveConfig 闭包内部完成校验+落盘+热应用；校验失败返回 400 且不写盘。
func (p *Panel) saveConfig(w http.ResponseWriter, r *http.Request) {
	if p.cfg.SaveConfig == nil {
		writeErr(w, http.StatusNotImplemented, "config api not available")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	restartRequired, err := p.cfg.SaveConfig(raw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if restartRequired == nil {
		restartRequired = []string{}
	}
	log.Printf("panel: 配置已保存（热生效完成；需重启字段 %d 个）", len(restartRequired))
	var managedFields []string
	if p.cfg.LoadConfig != nil {
		if config, err := p.cfg.LoadConfig(); err == nil {
			managedFields = configManagedFields(config)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                         true,
		"restart_required":           restartRequired,
		"api_key_env_managed":        os.Getenv("WB2A_API_KEY") != "",
		"environment_managed_fields": managedFields,
	})
}

// r2SyncNow 手动触发一轮「本地 → R2」同步（数据外置）。后台周期同步之外，
// 改完配置/加完账号想立刻把数据推上 R2（不等下一个间隔）时点它；同步结果
// 由 syncer 打进面板日志，这里只回报成败。
func (p *Panel) r2SyncNow(w http.ResponseWriter, r *http.Request) {
	if p.cfg.R2SyncNow == nil {
		writeErr(w, http.StatusNotImplemented, "r2 同步未启用")
		return
	}
	if err := p.cfg.R2SyncNow(); err != nil {
		writeErr(w, http.StatusInternalServerError, "r2 同步失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// configManagedFields 读取配置对象的「被启动环境变量托管字段」清单（可选能力：
// 配置类型未提供该方法时返回 nil）。移植上游 PR #104。
func configManagedFields(config any) []string {
	if provider, ok := config.(interface{ EnvironmentManagedFields() []string }); ok {
		return provider.EnvironmentManagedFields()
	}
	return nil
}
