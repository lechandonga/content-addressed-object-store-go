package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Options 控制 Store 的可选行为。
type Options struct {
	// ReadOnly 以只读模式打开（写入与回收将返回错误）。
	ReadOnly bool
}

// Version 描述对象的一个历史版本，是对某份内容寻址数据的不可变引用。
type Version struct {
	// ID 为单调递增的版本号（从 1 开始）。
	ID int64
	// Key 是该版本指向的数据内容键。
	Key ContentKey
	// Size 为数据字节数。
	Size int64
	// CreatedAt 为版本创建时间。
	CreatedAt time.Time
}

// versionMeta 是版本链中单个节点在磁盘上的持久化结构。
type versionMeta struct {
	ID        int64      `json:"id"`
	Key       ContentKey `json:"key"`
	Size      int64      `json:"size"`
	CreatedAt time.Time  `json:"created_at"`
}

// objectState 是单个对象目录下保存的元数据：完整版本链 + 当前版本指针。
// 该文件通过"临时文件 + fsync + 原子 rename"发布，外界永远看不到半成品。
type objectState struct {
	Current  int64         `json:"current"`
	Versions []versionMeta `json:"versions"`
}

// 仓库内部目录/文件名常量。
const (
	dirBlobs     = "blobs"
	dirTmp       = "tmp"
	dirObjects   = "objects"
	lockFileName = "LOCK"
	fileMeta     = "meta.json"
	fileCommit   = "commit.lock"
	metaPerm     = 0o644
	dirPerm      = 0o755
)

// Store 是本地内容寻址对象存储。零值不可用，须通过 Open 创建。
type Store struct {
	root     string       // 仓库根目录
	readOnly bool         // 是否只读模式
	lockPath string       // 跨进程访问协调锁文件路径（读写共享 / GC 独占）
	mu       sync.RWMutex // 协调本进程内 goroutine（与 flock 语义对齐）
}

// Open 打开（或初始化）位于 root 的对象存储。
func Open(root string, opts *Options) (*Store, error) {
	if root == "" {
		return nil, newError(KindIO, "store root must not be empty")
	}
	readOnly := opts != nil && opts.ReadOnly
	if !readOnly {
		for _, d := range []string{"", dirBlobs, dirTmp, dirObjects} {
			if err := os.MkdirAll(filepath.Join(root, d), dirPerm); err != nil {
				return nil, wrapError(KindIO, err, "init store layout under %q", root)
			}
		}
	}
	lockPath := filepath.Join(root, lockFileName)
	if !readOnly {
		f, err := os.OpenFile(lockPath, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return nil, wrapError(KindIO, err, "create lock file under %q", root)
		}
		_ = f.Close()
	}
	return &Store{
		root:     root,
		readOnly: readOnly,
		lockPath: lockPath,
	}, nil
}

// Close 释放存储资源。调用方须保证没有进行中的 Put/Get/GC
// （每次操作使用独立的锁文件描述符，操作结束即释放）。
func (s *Store) Close() error { return nil }

// beginRead 获取进程内读锁 + 跨进程共享锁（独立的锁文件描述符）。
// 所有读写操作都持有共享锁，因此彼此完全并行；GC 的独占锁与之互斥。
// 返回的 *fileLock 在操作结束（或读取流 Close）时必须交还 endRead。
func (s *Store) beginRead() (*fileLock, error) {
	s.mu.RLock()
	lk, err := newFileLock(s.lockPath, s.readOnly)
	if err != nil {
		s.mu.RUnlock()
		return nil, wrapError(KindIO, err, "open access lock")
	}
	if err := lk.rLock(); err != nil {
		_ = lk.Close()
		s.mu.RUnlock()
		return nil, err
	}
	return lk, nil
}

func (s *Store) endRead(lk *fileLock) {
	_ = lk.Close()
	s.mu.RUnlock()
}

// beginGC 获取进程内写锁 + 跨进程独占锁（独立的锁文件描述符）。
func (s *Store) beginGC() (*fileLock, error) {
	s.mu.Lock()
	lk, err := newFileLock(s.lockPath, s.readOnly)
	if err != nil {
		s.mu.Unlock()
		return nil, wrapError(KindIO, err, "open access lock")
	}
	if err := lk.lock(); err != nil {
		_ = lk.Close()
		s.mu.Unlock()
		return nil, err
	}
	return lk, nil
}

func (s *Store) endGC(lk *fileLock) {
	_ = lk.Close()
	s.mu.Unlock()
}

func (s *Store) checkWritable(op string) error {
	if s.readOnly {
		return newError(KindIO, "cannot %s: store opened read-only", op)
	}
	return nil
}

// objectDir 返回对象名对应的目录（对名字做 PathEscape，避免路径穿越与分隔符问题）。
func (s *Store) objectDir(name string) string {
	return filepath.Join(s.root, dirObjects, url.PathEscape(name))
}

// fsyncDir 对目录执行 fsync，保证此前的 rename/创建在崩溃后仍然持久。
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// writeTempFile 在仓库 tmp 目录中新建临时文件，向其写入 r 的全部内容并 fsync。
// 调用方负责在不再需要时关闭并删除该临时文件。
func (s *Store) writeTempFile(r io.Reader) (*os.File, int64, ContentKey, error) {
	f, err := os.CreateTemp(filepath.Join(s.root, dirTmp), "incoming-*")
	if err != nil {
		return nil, 0, "", wrapError(KindIO, err, "create temp file")
	}
	cleanup := func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}
	h := newHasher()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		cleanup()
		return nil, 0, "", wrapError(KindIO, err, "stream object data into temp file")
	}
	if err := f.Sync(); err != nil {
		cleanup()
		return nil, 0, "", wrapError(KindIO, err, "fsync temp file")
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, "", wrapError(KindIO, err, "rewind temp file")
	}
	return f, n, h.key(), nil
}

// commitBlob 将已落盘的临时文件发布为内容键 k 对应的数据块。
// 若数据块已存在（相同内容被重复写入），直接复用并删除临时文件 —— 只存一份。
// 发布通过同目录原子 rename 完成，发布后对目录 fsync。
func (s *Store) commitBlob(f *os.File, k ContentKey, size int64) error {
	if !k.valid() {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return newError(KindIO, "internal: invalid content key %q", k)
	}
	rel := k.shardPath()
	final := filepath.Join(s.root, dirBlobs, rel)
	if _, err := os.Stat(final); err == nil {
		// 内容相同（键即摘要），复用已有数据块，实现去重合并。
		_ = f.Close()
		return os.Remove(f.Name())
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return wrapError(KindIO, err, "stat blob %s", k)
	}
	if err := os.MkdirAll(filepath.Dir(final), dirPerm); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return wrapError(KindIO, err, "create blob shard dir")
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return wrapError(KindIO, err, "close temp blob")
	}
	if err := os.Rename(f.Name(), final); err != nil {
		_ = os.Remove(f.Name())
		return wrapError(KindIO, err, "publish blob %s", k)
	}
	if err := fsyncDir(filepath.Dir(final)); err != nil {
		return wrapError(KindIO, err, "fsync blob dir after publish")
	}
	return nil
}

// readObjectState 读取并解析对象的版本元数据。
// 元数据文件不存在 -> *NotFoundError；无法解析 -> KindCorrupt。
func (s *Store) readObjectState(name string) (*objectState, error) {
	p := filepath.Join(s.objectDir(name), fileMeta)
	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &NotFoundError{Obj: name}
		}
		return nil, wrapError(KindIO, err, "read meta of %q", name)
	}
	var st objectState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, wrapError(KindCorrupt, err, "object %q meta is corrupt", name)
	}
	if st.Current == 0 || len(st.Versions) == 0 {
		return nil, wrapError(KindCorrupt, fmt.Errorf("empty version chain"), "object %q meta is corrupt", name)
	}
	return &st, nil
}

// withCommitLock 对单个对象加跨进程独占提交锁，串行化同一对象的并发提交。
// 不同对象互不阻塞；读者不经过此锁（meta.json 的原子 rename 已保证读者一致）。
func (s *Store) withCommitLock(name string, fn func() error) error {
	dir := s.objectDir(name)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return wrapError(KindIO, err, "create object dir for %q", name)
	}
	lk, err := newFileLock(filepath.Join(dir, fileCommit), false)
	if err != nil {
		return err
	}
	defer lk.Close()
	if err := lk.lock(); err != nil {
		return err
	}
	return fn()
}

// writeMetaAtomic 将对象元数据以 临时文件+fsync+rename 的方式原子发布。
func (s *Store) writeMetaAtomic(name string, st *objectState) error {
	dir := s.objectDir(name)
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return wrapError(KindIO, err, "encode meta of %q", name)
	}
	tmp, err := os.CreateTemp(dir, "meta-*.tmp")
	if err != nil {
		return wrapError(KindIO, err, "create meta temp for %q", name)
	}
	tmpName := tmp.Name()
	abort := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		abort()
		return wrapError(KindIO, err, "write meta temp for %q", name)
	}
	if err := tmp.Sync(); err != nil {
		abort()
		return wrapError(KindIO, err, "fsync meta temp for %q", name)
	}
	if err := tmp.Close(); err != nil {
		abort()
		return wrapError(KindIO, err, "close meta temp for %q", name)
	}
	final := filepath.Join(dir, fileMeta)
	if err := os.Rename(tmpName, final); err != nil {
		_ = os.Remove(tmpName)
		return wrapError(KindIO, err, "publish meta for %q", name)
	}
	if err := fsyncDir(dir); err != nil {
		return wrapError(KindIO, err, "fsync object dir for %q", name)
	}
	return nil
}

var _ = context.Background
