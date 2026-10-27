// Package r2sync 把项目数据文件镜像到 Cloudflare R2（经自建 Worker 文件接口）。
//
// Worker 协议（部署说明与加固版脚本见 README「数据外置到 Cloudflare R2」）：
//
//	GET    /             Accept: application/json → 对象列表（JSON 数组）
//	GET    /<key>        下载对象（原版脚本不校验鉴权头，客户端仍总是携带）
//	PUT    /<key>        上传对象，需 X-Custom-Auth-Key
//	DELETE /<key>        删除对象，需 X-Custom-Auth-Key
//
// 为什么需要这一层：「数据不能外挂（挂载外部卷）」的部署形态里本地磁盘只是缓存，
// 重启即空。启动时 Restore 先把 R2 上的 config/auths/data 拉回本地（组件按原有
// 路径照常读取），运行期后台把变更推回 R2；请求归档按保留策略删除本地分片时，
// 同步删除 R2 副本，否则远端会无限增长、重启时又全量拉回撑爆小磁盘。
package r2sync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrNotFound 对象不存在（Worker 对缺失 key 返回 404）。
var ErrNotFound = errors.New("r2sync: object not found")

// Client Worker 文件接口客户端。
type Client struct {
	baseURL string // 去掉结尾 "/"
	authKey string
	prefix  string // 归一化后的 key 前缀：空或以 "/" 结尾
	http    *http.Client
}

// NormalizePrefix 归一化 key 前缀：去两端空白，去掉首尾 "/"，非空时补结尾 "/"。
// 空串表示直接用桶根（不推荐：Worker 上传/删除是整桶范围，会与手动管理的文件混在一起）。
func NormalizePrefix(p string) string {
	p = strings.Trim(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

// NewClient 创建客户端。baseURL 为 Worker 地址（https://xxx.workers.dev）。
func NewClient(baseURL, authKey, prefix string) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		authKey: strings.TrimSpace(authKey),
		prefix:  NormalizePrefix(prefix),
		http:    &http.Client{Timeout: 60 * time.Second},
	}
}

// Prefix 返回归一化后的 key 前缀。
func (c *Client) Prefix() string { return c.prefix }

// Key 拼出远端 key：<prefix><tag>/<rel>。rel 用 "/" 分隔（Windows 本地路径需先转斜杠）。
func (c *Client) Key(tag, rel string) string {
	return c.prefix + tag + "/" + rel
}

// Object 列表项。Uploaded 为 R2 对象的最后上传时间，供「本地/远端谁更新」比较。
type Object struct {
	Key      string
	Size     int64
	Uploaded time.Time
}

// listItem Worker 返回的原始列表项（R2 list.objects 的字段子集）。
type listItem struct {
	Key      string `json:"key"`
	Size     int64  `json:"size"`
	Uploaded string `json:"uploaded"`
}

// List 列出桶内全部对象（Worker 不支持前缀参数，由调用方按 Prefix 过滤）。
// 原版 Worker 对 GET / 的 Accept 头做 strings.includes 判断，缺 Accept 会直接 500，
// 所以这里必须显式带上 application/json。
func (c *Client) List(ctx context.Context) ([]Object, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Custom-Auth-Key", c.authKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("r2sync: list: %s: %s", resp.Status, snippet(body))
	}
	// 兼容两种返回形态：[{...}]（原版脚本）与 {"objects":[{...}]}（部分改写版）。
	var items []listItem
	if len(body) > 0 && body[0] == '[' {
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("r2sync: list: 解析列表: %w", err)
		}
	} else {
		var wrapped struct {
			Objects []listItem `json:"objects"`
		}
		if err := json.Unmarshal(body, &wrapped); err != nil {
			return nil, fmt.Errorf("r2sync: list: 解析列表: %w", err)
		}
		items = wrapped.Objects
	}
	out := make([]Object, 0, len(items))
	for _, it := range items {
		o := Object{Key: it.Key, Size: it.Size}
		// uploaded 解析失败按零值处理：恢复时只会在本地缺失时拉取，不会用坏时间戳
		// 覆盖较新的本地文件。
		if it.Uploaded != "" {
			if t, err := time.Parse(time.RFC3339Nano, it.Uploaded); err == nil {
				o.Uploaded = t
			}
		}
		out = append(out, o)
	}
	return out, nil
}

// Get 下载对象；不存在返回 ErrNotFound。
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")
	req.Header.Set("X-Custom-Auth-Key", c.authKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("r2sync: get %q: %s: %s", key, resp.Status, snippet(body))
	}
	return body, nil
}

// Put 上传/覆盖对象。
func (c *Client) Put(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Custom-Auth-Key", c.authKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("r2sync: put %q: %s: %s", key, resp.Status, snippet(body))
	}
	return nil
}

// Delete 删除对象；对象本就不存在视为成功（幂等）。
func (c *Client) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(key), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Custom-Auth-Key", c.authKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("r2sync: delete %q: %s: %s", key, resp.Status, snippet(body))
	}
	return nil
}

// objectURL 拼对象 URL：逐段 PathEscape，保留 "/" 分隔（Worker 侧 decodeURI 还原）。
func (c *Client) objectURL(key string) string {
	segs := strings.Split(key, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return c.baseURL + "/" + strings.Join(segs, "/")
}

// snippet 截断错误响应体，避免把整页 HTML 打进日志。
func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
