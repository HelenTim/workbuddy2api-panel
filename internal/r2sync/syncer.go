// syncer.go 文件镜像：启动从 R2 恢复、后台把本地变更推回、本地分片被删时同步删远端。
package r2sync

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/misakano7545/workbuddy2api-panel/internal/safefile"
)

// Options 同步范围与策略。三类 root 各自映射到远端 key 前缀下的一个 tag 目录：
//
//	config → <prefix>config/<配置文件基名>    （单文件，恢复时总是落回 ConfigFile）
//	auth   → <prefix>auth/<auth 目录内相对路径>
//	state  → <prefix>state/<state 目录内相对路径>（含 request-logs/ 子目录）
//
// 空字符串表示不参与同步。
type Options struct {
	ConfigFile string
	AuthDir    string
	StateDir   string // state.json 所在目录（usage/余额/积分历史/请求归档都在这里）
	// Interval 后台同步间隔；<=0 回落 1 分钟。
	Interval time.Duration
	// DeleteRemote 本地文件消失时是否删除远端副本。请求归档按保留策略轮换时
	// 必须开启，否则远端无限增长、下次启动又全量拉回；代价是「把 state_file /
	// auth_dir 改到新路径」会让旧路径下的远端数据被当作已消失而删除。
	DeleteRemote bool
}

// stamp 上次成功同步时的文件指纹（大小 + mtime）。state 文件走 safefile 的
// rename 替换、归档文件走 O_APPEND，两者都会更新 mtime，足够判断变更。
type stamp struct {
	size  int64
	mtime time.Time
}

// Syncer 并发安全的文件镜像器：Restore 只在启动时调用一次，之后由 Start 的后台
// goroutine 与 Close 的最后一次刷盘调用 SyncNow。
type Syncer struct {
	client *Client
	opts   Options

	mu      sync.Mutex
	sent    map[string]stamp // 已成功上传/拉回且仍在本地的 key
	started bool
	stop    chan struct{}
	done    chan struct{}

	closeOnce sync.Once
	failMu    sync.Mutex
	fails     int
}

// New 创建镜像器。client 与至少一个 root 都非空才有意义；调用方按配置是否启用决定。
func New(client *Client, opts Options) *Syncer {
	return &Syncer{
		client: client,
		opts:   opts,
		sent:   make(map[string]stamp),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

type root struct {
	tag  string
	path string
	file bool
}

func (s *Syncer) roots() []root {
	var rs []root
	if s.opts.ConfigFile != "" {
		rs = append(rs, root{tag: "config", path: s.opts.ConfigFile, file: true})
	}
	if s.opts.AuthDir != "" {
		rs = append(rs, root{tag: "auth", path: s.opts.AuthDir})
	}
	if s.opts.StateDir != "" {
		rs = append(rs, root{tag: "state", path: s.opts.StateDir})
	}
	return rs
}

// Restore 启动恢复：对每个远端对象，本地缺失或比远端旧时拉回本地。
//
// 本地更新（mtime 晚于 uploaded）时保持不动，留给 SyncNow 把它推上去——这条
// 「本地优先」规则让持久磁盘部署不被远端旧副本回滚。单个文件失败只记日志、
// 不中断其余恢复；返回聚合错误供调用方打 WARN。
func (s *Syncer) Restore(ctx context.Context) error {
	objs, err := s.client.List(ctx)
	if err != nil {
		return err
	}
	if len(objs) >= 1000 {
		// Worker 的 list() 单次最多返回 1000 个对象；超出时远端数据可能不全。
		log.Printf("[r2] WARN: 远端列表达到 1000 项上限，可能被截断（桶内对象过多？）")
	}
	var pulled, kept, failed int
	for _, o := range objs {
		if !strings.HasPrefix(o.Key, s.client.Prefix()) {
			continue // 前缀之外的手动文件不归本程序管
		}
		path, ok := s.localPath(o.Key)
		if !ok {
			continue
		}
		info, statErr := os.Stat(path)
		if statErr == nil && !o.Uploaded.After(info.ModTime()) {
			kept++
			continue
		}
		data, gerr := s.client.Get(ctx, o.Key)
		if gerr != nil {
			failed++
			log.Printf("[r2] 恢复 %s 失败: %v", o.Key, gerr)
			continue
		}
		if werr := writeLocal(path, data); werr != nil {
			failed++
			log.Printf("[r2] 恢复 %s 写本地失败: %v", o.Key, werr)
			continue
		}
		if info, ierr := os.Stat(path); ierr == nil {
			s.markSent(o.Key, info.Size(), info.ModTime())
		}
		pulled++
	}
	if pulled > 0 || failed > 0 {
		log.Printf("[r2] 启动恢复：拉回 %d、本地较新保留 %d、失败 %d（前缀 %q）",
			pulled, kept, failed, s.client.Prefix())
	}
	if failed > 0 {
		return fmt.Errorf("r2sync: 启动恢复有 %d 个文件失败", failed)
	}
	return nil
}

// Start 启动后台周期同步。ctx 取消（进程停机）时 goroutine 退出；Close 负责
// 停机前最后一次刷盘。
func (s *Syncer) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	interval := s.opts.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		defer close(s.done)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				s.runOnce(ctx)
			case <-s.stop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Close 停后台循环并做最后一次同步（等待停机时已落盘的状态文件写进 R2）。
// 幂等。注册为 defer 时要早于 pool/requestLog 的 Close，LIFO 才能让最后一批
// 落盘文件被收进这次同步。
func (s *Syncer) Close() {
	s.closeOnce.Do(func() {
		close(s.stop)
		s.mu.Lock()
		started := s.started
		s.mu.Unlock()
		if started {
			<-s.done
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := s.SyncNow(ctx); err != nil {
			log.Printf("[r2] 退出前最后一次同步失败: %v", err)
		}
	})
}

func (s *Syncer) runOnce(parent context.Context) {
	timeout := s.opts.Interval
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	if err := s.SyncNow(ctx); err != nil {
		s.logFailure(err)
		return
	}
	s.failMu.Lock()
	recovered := s.fails
	s.fails = 0
	s.failMu.Unlock()
	if recovered > 0 {
		log.Printf("[r2] 同步恢复（此前连续失败 %d 次）", recovered)
	}
}

// SyncNow 执行一轮「本地 → 远端」：变化的文件上传，本地已删除的旧文件删远端。
// 失败不回滚已成功的项，下一轮按指纹重试；返回首个错误。
func (s *Syncer) SyncNow(ctx context.Context) error {
	files := s.localFiles()
	var firstErr error
	uploaded := 0
	for key, f := range files {
		s.mu.Lock()
		prev, ok := s.sent[key]
		s.mu.Unlock()
		if ok && prev.size == f.size && prev.mtime.Equal(f.mtime) {
			continue
		}
		data, err := os.ReadFile(f.path)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := s.client.Put(ctx, key, data); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// 读后再取一次 stat：上传期间归档文件可能又被追加，记录最新戳，
		// 下一轮会补传增量而不是把这个戳当成「已同步」漏掉尾部。
		if info, ierr := os.Stat(f.path); ierr == nil {
			s.markSent(key, info.Size(), info.ModTime())
		} else {
			s.markSent(key, f.size, f.mtime)
		}
		uploaded++
	}

	deleted := 0
	if s.opts.DeleteRemote {
		s.mu.Lock()
		var stale []string
		for key := range s.sent {
			if _, ok := files[key]; !ok {
				stale = append(stale, key)
			}
		}
		s.mu.Unlock()
		for _, key := range stale {
			if err := s.client.Delete(ctx, key); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			s.mu.Lock()
			delete(s.sent, key)
			s.mu.Unlock()
			deleted++
		}
	}
	if uploaded > 0 || deleted > 0 {
		log.Printf("[r2] 同步：上传 %d、删除 %d", uploaded, deleted)
	}
	return firstErr
}

// logFailure 连续失败日志节流：首败打完整错误，之后每 12 次一条（60s 间隔 ≈ 12 分钟）。
func (s *Syncer) logFailure(err error) {
	s.failMu.Lock()
	s.fails++
	n := s.fails
	s.failMu.Unlock()
	if n == 1 || n%12 == 0 {
		log.Printf("[r2] 同步失败（连续第 %d 次）: %v", n, err)
	}
}

func (s *Syncer) markSent(key string, size int64, mtime time.Time) {
	s.mu.Lock()
	s.sent[key] = stamp{size: size, mtime: mtime}
	s.mu.Unlock()
}

type localFile struct {
	path  string
	size  int64
	mtime time.Time
}

// localFiles 扫描全部 root，返回「远端 key → 本地文件指纹」。
// 跳过隐藏文件/目录（safefile 的 .<name>.* 临时文件）与 *.tmp；目录不可读时
// 静默跳过，不阻断其它 root 的同步。
func (s *Syncer) localFiles() map[string]localFile {
	files := make(map[string]localFile)
	for _, r := range s.roots() {
		if r.file {
			info, err := os.Stat(r.path)
			if err != nil || info.IsDir() {
				continue
			}
			base := filepath.Base(r.path)
			if skipName(base) {
				continue
			}
			files[s.client.Key(r.tag, base)] = localFile{path: r.path, size: info.Size(), mtime: info.ModTime()}
			continue
		}
		rootPath := r.path
		_ = filepath.WalkDir(rootPath, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				if path == rootPath {
					return filepath.SkipAll // 根目录不可读：整棵跳过
				}
				return nil
			}
			if path == rootPath {
				return nil
			}
			name := d.Name()
			if d.IsDir() {
				if skipName(name) {
					return filepath.SkipDir
				}
				return nil
			}
			if skipName(name) || strings.HasSuffix(name, ".tmp") {
				return nil
			}
			info, ierr := d.Info()
			if ierr != nil {
				return nil
			}
			rel, rerr := filepath.Rel(rootPath, path)
			if rerr != nil {
				return nil
			}
			files[s.client.Key(r.tag, filepath.ToSlash(rel))] = localFile{path: path, size: info.Size(), mtime: info.ModTime()}
			return nil
		})
	}
	return files
}

// skipName 隐藏文件/目录（safefile 临时文件形如 .state.json.123）不参与同步。
func skipName(name string) bool { return strings.HasPrefix(name, ".") }

// localPath 把远端 key 映射回本地路径。config tag 总是落回 ConfigFile（与基名无关，
// 便于配置路径改名后仍能恢复）；auth/state 保留相对路径并做穿越防护。
func (s *Syncer) localPath(key string) (string, bool) {
	rel, ok := strings.CutPrefix(key, s.client.Prefix())
	if !ok {
		return "", false
	}
	tag, rest, ok := strings.Cut(rel, "/")
	if !ok {
		return "", false
	}
	switch tag {
	case "config":
		if s.opts.ConfigFile == "" {
			return "", false
		}
		return s.opts.ConfigFile, true
	case "auth":
		if s.opts.AuthDir == "" || !safeRel(rest) {
			return "", false
		}
		return filepath.Join(s.opts.AuthDir, filepath.FromSlash(rest)), true
	case "state":
		if s.opts.StateDir == "" || !safeRel(rest) {
			return "", false
		}
		return filepath.Join(s.opts.StateDir, filepath.FromSlash(rest)), true
	}
	return "", false
}

// safeRel 拒绝绝对路径与逃出 root 的 ".." 段。
func safeRel(rel string) bool {
	if rel == "" || strings.HasPrefix(rel, "/") || filepath.IsAbs(rel) {
		return false
	}
	clean := filepath.Clean(filepath.FromSlash(rel))
	return clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator))
}

// writeLocal 原子落盘（safefile：tmp → fsync → rename，掉电不留半截文件）。
// 不用 WriteKeep：请求归档是 JSONL（整体不是合法 JSON），keep 会把每份旧文件
// 都当作损坏另存 .bad-*，制造垃圾。
func writeLocal(path string, data []byte) error {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return safefile.Write(path, data, 0o600)
}
