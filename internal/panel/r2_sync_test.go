package panel

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestR2SyncNowEndpoint 「立即同步」按钮的后端行为：
//   - 未注入回调（R2 未启用）→ 501，前端据此隐藏按钮
//   - 注入后调用成功 → 200 {ok:true}
//   - 同步报错（如 Worker 不可达）→ 500 且带错误文本
func TestR2SyncNowEndpoint(t *testing.T) {
	p := New(Config{APIKey: "k"})
	req := httptest.NewRequest(http.MethodPost, "/panel/api/r2_sync", nil)
	req.Header.Set("Authorization", "Bearer k")
	rr := httptest.NewRecorder()
	p.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("未启用时应 501，得到 %d", rr.Code)
	}

	var called int
	p2 := New(Config{APIKey: "k", R2SyncNow: func() error {
		called++
		if called == 1 {
			return nil
		}
		return errors.New("worker unreachable")
	}})
	for want, doSync := range map[int]bool{} {
		_ = want
		_ = doSync
	}
	// 成功路径
	rr = httptest.NewRecorder()
	p2.ServeHTTP(rr, mustReq(t, "/panel/api/r2_sync", "k"))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"ok":true`) {
		t.Fatalf("成功同步应 200，得到 %d %s", rr.Code, rr.Body.String())
	}
	// 失败路径
	rr = httptest.NewRecorder()
	p2.ServeHTTP(rr, mustReq(t, "/panel/api/r2_sync", "k"))
	if rr.Code != http.StatusInternalServerError || !strings.Contains(rr.Body.String(), "worker unreachable") {
		t.Fatalf("同步失败应 500 带错误文本，得到 %d %s", rr.Code, rr.Body.String())
	}
	if called != 2 {
		t.Fatalf("回调应被调用 2 次，实际 %d", called)
	}
}

// TestGetConfigReportsR2Enabled 配置回显必须带 r2_enabled：前端据此决定
// 「立即同步」按钮显隐，漏掉会让启用外置的部署永远看不到按钮。
func TestGetConfigReportsR2Enabled(t *testing.T) {
	p := New(Config{
		APIKey:      "k",
		R2Enabled:   func() bool { return true },
		LoadConfig:  func() (any, error) { return map[string]any{"listen": ":7863"}, nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/panel/api/config", nil)
	req.Header.Set("Authorization", "Bearer k")
	p.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"r2_enabled":true`) {
		t.Fatalf("r2_enabled 未透出: %d %s", rr.Code, rr.Body.String())
	}
}

func mustReq(t *testing.T, path, key string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.Header.Set("Authorization", "Bearer "+key)
	if strings.Contains(path, "/config") {
		req.Method = http.MethodGet
	}
	return req
}
