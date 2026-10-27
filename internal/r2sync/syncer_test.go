package r2sync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeWorker 按用户 Cloudflare Worker 的协议实现内存版 R2 桶：
// GET / 列表（Accept 缺省会像原版脚本一样 500）、GET /<key> 下载、
// PUT/DELETE 需要 X-Custom-Auth-Key。
type fakeWorker struct {
	mu      sync.Mutex
	objs    map[string]fakeObj
	authKey string
	puts    int
	gets    int
	deletes int
}

type fakeObj struct {
	data     []byte
	uploaded time.Time
}

func newFakeWorker(authKey string) *fakeWorker {
	return &fakeWorker{objs: map[string]fakeObj{}, authKey: authKey}
}

func (f *fakeWorker) put(key string, data []byte, uploaded time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objs[key] = fakeObj{data: append([]byte(nil), data...), uploaded: uploaded}
}

func (f *fakeWorker) get(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.objs[key]
	return o.data, ok
}

func (f *fakeWorker) has(key string) bool {
	_, ok := f.get(key)
	return ok
}

func (f *fakeWorker) counts() (puts, gets, deletes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.puts, f.gets, f.deletes
}

func (f *fakeWorker) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if r.URL.Path == "/" {
			accept := r.Header.Get("Accept")
			if !strings.Contains(accept, "text/html") && accept == "" {
				// 原版 Worker：request.headers.get('Accept').includes(...) 对 null 抛错。
				http.Error(w, "TypeError: Cannot read properties of null", http.StatusInternalServerError)
				return
			}
			if strings.Contains(accept, "text/html") {
				_, _ = io.WriteString(w, "<html>file manager</html>")
				return
			}
			f.mu.Lock()
			items := make([]map[string]any, 0, len(f.objs))
			for key, o := range f.objs {
				items = append(items, map[string]any{
					"key": key, "size": len(o.data), "uploaded": o.uploaded.UTC().Format(time.RFC3339Nano),
				})
			}
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/")
		f.mu.Lock()
		o, ok := f.objs[key]
		f.gets++
		f.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(o.data)
	case http.MethodPut:
		if r.Header.Get("X-Custom-Auth-Key") != f.authKey {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		data, _ := io.ReadAll(r.Body)
		key := strings.TrimPrefix(r.URL.Path, "/")
		f.mu.Lock()
		f.objs[key] = fakeObj{data: data, uploaded: time.Now()}
		f.puts++
		f.mu.Unlock()
		_, _ = io.WriteString(w, "File uploaded successfully")
	case http.MethodDelete:
		if r.Header.Get("X-Custom-Auth-Key") != f.authKey {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/")
		f.mu.Lock()
		delete(f.objs, key)
		f.deletes++
		f.mu.Unlock()
		_, _ = io.WriteString(w, "File deleted successfully")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func testSyncer(t *testing.T, fw *fakeWorker, prefix string) (*Syncer, string, string, string) {
	t.Helper()
	srv := httptest.NewServer(fw)
	t.Cleanup(srv.Close)
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	stateDir := filepath.Join(root, "data")
	cfgPath := filepath.Join(root, "config.json")
	s := New(NewClient(srv.URL, "secret", prefix), Options{
		ConfigFile:   cfgPath,
		AuthDir:      authDir,
		StateDir:     stateDir,
		Interval:     time.Minute,
		DeleteRemote: true,
	})
	return s, authDir, stateDir, cfgPath
}

func TestClientRoundTrip(t *testing.T) {
	fw := newFakeWorker("secret")
	srv := httptest.NewServer(fw)
	defer srv.Close()
	c := NewClient(srv.URL, "secret", "p/")
	ctx := context.Background()

	if err := c.Put(ctx, "p/auth/a.json", []byte(`{"uid":"a"}`)); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := c.Get(ctx, "p/auth/a.json")
	if err != nil || string(got) != `{"uid":"a"}` {
		t.Fatalf("get = %q, %v", got, err)
	}
	objs, err := c.List(ctx)
	if err != nil || len(objs) != 1 || objs[0].Key != "p/auth/a.json" || objs[0].Size == 0 || objs[0].Uploaded.IsZero() {
		t.Fatalf("list = %+v, %v", objs, err)
	}
	if err := c.Delete(ctx, "p/auth/a.json"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := c.Get(ctx, "p/auth/a.json"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing get err = %v, want ErrNotFound", err)
	}

	bad := NewClient(srv.URL, "wrong", "p/")
	if err := bad.Put(ctx, "p/x", []byte("x")); err == nil {
		t.Fatal("鉴权错误的 PUT 应当失败")
	}
}

func TestRestorePullsWhenLocalMissingOrOlder(t *testing.T) {
	fw := newFakeWorker("secret")
	s, authDir, stateDir, cfgPath := testSyncer(t, fw, "wb2a/")

	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// 本地 state.json 较旧 → 应被远端覆盖。
	localState := filepath.Join(stateDir, "state.json")
	if err := os.WriteFile(localState, []byte(`{"v":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(localState, past, past); err != nil {
		t.Fatal(err)
	}
	fw.put("wb2a/state/state.json", []byte(`{"v":2}`), time.Now())
	// 本地缺失的 auth 文件与 config → 应拉回。
	fw.put("wb2a/auth/a.json", []byte(`{"uid":"a"}`), time.Now())
	fw.put("wb2a/config/config.json", []byte(`{"listen":":1"}`), time.Now())
	// 前缀之外的对象不属于本程序 → 忽略。
	fw.put("other/keep.txt", []byte("keep"), time.Now())

	if err := s.Restore(context.Background()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if b := readFile(t, localState); b != `{"v":2}` {
		t.Fatalf("state.json = %q, want remote newer content", b)
	}
	if b := readFile(t, filepath.Join(authDir, "a.json")); b != `{"uid":"a"}` {
		t.Fatalf("a.json = %q", b)
	}
	if b := readFile(t, cfgPath); b != `{"listen":":1"}` {
		t.Fatalf("config.json = %q", b)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(stateDir), "keep.txt")); !os.IsNotExist(err) {
		t.Fatal("前缀外的对象不应被恢复")
	}
}

func TestRestoreKeepsNewerLocal(t *testing.T) {
	fw := newFakeWorker("secret")
	s, _, stateDir, _ := testSyncer(t, fw, "wb2a/")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	localState := filepath.Join(stateDir, "state.json")
	if err := os.WriteFile(localState, []byte(`{"v":"local"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fw.put("wb2a/state/state.json", []byte(`{"v":"remote"}`), time.Now().Add(-time.Hour))

	if err := s.Restore(context.Background()); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if b := readFile(t, localState); b != `{"v":"local"}` {
		t.Fatalf("state.json = %q, want local content kept", b)
	}
}

func TestSyncUploadsChangesAndDeletes(t *testing.T) {
	fw := newFakeWorker("secret")
	s, authDir, stateDir, cfgPath := testSyncer(t, fw, "wb2a/")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(stateDir, "request-logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, []byte(`{"listen":":7863"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	authFile := filepath.Join(authDir, "a.json")
	if err := os.WriteFile(authFile, []byte(`{"uid":"a"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	logFile := filepath.Join(stateDir, "request-logs", "requests-2026-10-27.jsonl")
	if err := os.WriteFile(logFile, []byte("{\"request_id\":\"r1\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.SyncNow(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	for _, key := range []string{"wb2a/config/config.json", "wb2a/auth/a.json", "wb2a/state/request-logs/requests-2026-10-27.jsonl"} {
		if !fw.has(key) {
			t.Fatalf("缺少远端对象 %s", key)
		}
	}
	putsAfterFirst, _, _ := fw.counts()
	if err := s.SyncNow(ctx); err != nil {
		t.Fatalf("sync again: %v", err)
	}
	if putsAfterSecond, _, _ := fw.counts(); putsAfterSecond != putsAfterFirst {
		t.Fatalf("未变化的文件不应重传: %d -> %d", putsAfterFirst, putsAfterSecond)
	}

	// 内容变化（mtime 随写入更新）→ 重传。
	if err := os.WriteFile(authFile, []byte(`{"uid":"a","v":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncNow(ctx); err != nil {
		t.Fatalf("sync change: %v", err)
	}
	if b, _ := fw.get("wb2a/auth/a.json"); string(b) != `{"uid":"a","v":2}` {
		t.Fatalf("远端内容未更新: %q", b)
	}

	// 本地删除（归档轮换）→ 远端同步删除；前缀外对象不受影响。
	fw.put("other/keep.txt", []byte("keep"), time.Now())
	if err := os.Remove(logFile); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncNow(ctx); err != nil {
		t.Fatalf("sync delete: %v", err)
	}
	if fw.has("wb2a/state/request-logs/requests-2026-10-27.jsonl") {
		t.Fatal("本地删除后远端副本应被删除")
	}
	if !fw.has("other/keep.txt") {
		t.Fatal("前缀外的对象不应被删除")
	}
}

func TestSyncSkipsTempAndHidden(t *testing.T) {
	fw := newFakeWorker("secret")
	s, _, stateDir, _ := testSyncer(t, fw, "wb2a/")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		".state.json.123": "tmp",
		"up.tmp":          "tmp",
		"state.json":      `{"ok":true}`,
	} {
		if err := os.WriteFile(filepath.Join(stateDir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SyncNow(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if fw.has("wb2a/state/.state.json.123") || fw.has("wb2a/state/up.tmp") {
		t.Fatal("隐藏/临时文件不应同步")
	}
	if !fw.has("wb2a/state/state.json") {
		t.Fatal("正常文件应同步")
	}
}

func TestLocalPathRejectsTraversal(t *testing.T) {
	s := New(NewClient("http://example.invalid", "k", "p/"), Options{AuthDir: "auths", StateDir: "data"})
	for _, key := range []string{"p/auth/../../../etc/passwd", "p/state/../x", "p/auth//abs"} {
		if _, ok := s.localPath(key); ok {
			t.Fatalf("%q 不应映射出 root 之外的本地路径", key)
		}
	}
	if p, ok := s.localPath("p/state/request-logs/r.jsonl"); !ok || !strings.HasSuffix(p, filepath.Join("data", "request-logs", "r.jsonl")) {
		t.Fatalf("正常 key 映射失败: %q %v", p, ok)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
