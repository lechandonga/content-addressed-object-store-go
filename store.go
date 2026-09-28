package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Version 描述某个键的一次写入结果，也是读回时的版本凭证。
type Version struct {
	Key       string    `json:"key"`
	Hash      string    `json:"hash"` // 内容地址（SHA-256 十六进制）
	Size      int64     `json:"size"` // 原始字节数
	CreatedAt time.Time `json:"created_at"`
	Index     int       `json:"index"` // 从 0 开始的版本序号
}

// versionRecord 是单个版本的持久化格式（序号由数组位置隐含）。
type versionRecord struct {
	Hash      string    `json:"hash"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

// keyMeta 是每个键的元数据文件格式：一条只追加的版本链。
type keyMeta struct {
	Key      string          `json:"key"`
	Versions []versionRecord `json:"versions"`
}

// 存储目录布局：
//
//	root/
//	  objects/<前2位>/<其余62位>   内容寻址的不可变数据块
//	  meta/<前2位>/<其余>.json     以 sha256(key) 命名的键元数据
//	  tmp/*.tmp                    写入过程中的临时文件（崩溃残留由 GC 清理）
const (
	objectsDir = "objects"
	metaDir    = "meta"
	tmpDir     = "tmp"
)

// Store 是本地内容寻址对象存储的句柄。
// 并发安全仅在同一进程内的多个 goroutine 之间保证。
type Store struct {
	root string
	mu   sync.RWMutex
}

// Open 打开（或初始化）位于 root 的存储，目录会按需创建。
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("%w: empty root path", ErrInvalidArgument)
	}
	for _, d := range []string{objectsDir, metaDir, tmpDir} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, fmt.Errorf("cas: init directory %q: %w", d, err)
		}
	}
	return &Store{root: root}, nil
}

// Root 返回存储根目录（主要用于测试与排障）。
func (s *Store) Root() string { return s.root }

// Close 释放存储资源。当前实现没有需要显式释放的资源。
func (s *Store) Close() error { return nil }

// ---- 路径与元数据辅助 ----

func objectPath(root, addr string) string {
	return filepath.Join(root, objectsDir, addr[:2], addr[2:])
}

func metaPath(root, key string) string {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:])
	return filepath.Join(root, metaDir, name[:2], name[2:]+".json")
}

// loadMeta 读取键的元数据；键不存在时返回 (nil, ErrNotFound)。
func (s *Store) loadMeta(key string) (*keyMeta, error) {
	f, err := os.Open(metaPath(s.root, key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %q", ErrNotFound, key)
		}
		return nil, fmt.Errorf("cas: load meta for %q: %w", key, err)
	}
	defer f.Close()
	var m keyMeta
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, fmt.Errorf("cas: decode meta for %q: %w", key, err)
	}
	return &m, nil
}

// saveMeta 以“临时文件 + 原子改名”替换元数据文件，
// 任何时刻外界都不会读到只写了一半的元数据。
func (s *Store) saveMeta(m *keyMeta) error {
	final := metaPath(s.root, m.Key)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, tmpDir), "meta-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		os.Remove(tmpName)
		return err
	}
	fsyncDir(filepath.Dir(final))
	return nil
}

// fsyncDir 尽力持久化目录项变更（崩溃恢复用，失败不影响正确性）。
func fsyncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// toVersion 把持久化记录转成对外版本信息。
func (m *keyMeta) toVersion(i int) Version {
	r := m.Versions[i]
	return Version{
		Key:       m.Key,
		Hash:      r.Hash,
		Size:      r.Size,
		CreatedAt: r.CreatedAt,
		Index:     i,
	}
}

// ---- 写入 ----

// Put 写入一个对象并返回新版本。
//
// 去重：数据块以内容摘要命名，相同内容只落盘一份。
// 版本：每次 Put 都向该键的版本链追加一条记录，历史版本不变。
// 原子性：数据先写入 tmp/ 临时文件并 fsync，再原子改名为其内容地址，
// 最后原子替换元数据文件；整个过程持写锁，外界看不到半成品。
func (s *Store) Put(key string, r io.Reader) (Version, error) {
	if key == "" {
		return Version{}, fmt.Errorf("%w: empty key", ErrInvalidArgument)
	}
	if r == nil {
		return Version{}, fmt.Errorf("%w: nil reader", ErrInvalidArgument)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.putLocked(key, r)
}

// putLocked 要求持有 s.mu 写锁。
func (s *Store) putLocked(key string, r io.Reader) (Version, error) {
	// 1) 流式写入 tmp/ 临时文件，同时计算 SHA-256，写完 fsync。
	tmp, err := os.CreateTemp(filepath.Join(s.root, tmpDir), "obj-*.tmp")
	if err != nil {
		return Version{}, fmt.Errorf("cas: create temp: %w", err)
	}
	tmpName := tmp.Name()
	abort := true
	defer func() {
		if abort {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		return Version{}, fmt.Errorf("cas: spool object: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return Version{}, fmt.Errorf("cas: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return Version{}, fmt.Errorf("cas: close temp: %w", err)
	}
	addr := hex.EncodeToString(h.Sum(nil))

	// 2) 原子改名为内容地址。文件已存在说明内容相同（去重命中），
	//    直接丢弃临时文件即可，数据块不可变，绝不会覆盖出不同内容。
	final := objectPath(s.root, addr)
	if _, err := os.Stat(final); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return Version{}, fmt.Errorf("cas: stat object: %w", err)
		}
		if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
			return Version{}, err
		}
		if err := os.Rename(tmpName, final); err != nil {
			return Version{}, fmt.Errorf("cas: publish object: %w", err)
		}
		fsyncDir(filepath.Dir(final))
	} else {
		_ = os.Remove(tmpName)
	}

	// 3) 加载或创建键元数据并追加新版本，最后原子替换元数据文件。
	m, err := s.loadMeta(key)
	if errors.Is(err, ErrNotFound) {
		m = &keyMeta{Key: key}
	} else if err != nil {
		return Version{}, err
	}
	rec := versionRecord{Hash: addr, Size: size, CreatedAt: time.Now().UTC()}
	m.Versions = append(m.Versions, rec)
	if err := s.saveMeta(m); err != nil {
		return Version{}, fmt.Errorf("cas: commit meta: %w", err)
	}

	abort = false
	return m.toVersion(len(m.Versions) - 1), nil
}

// ---- 读取 ----

// Get 读取键的最新版本。返回的 ReadCloser 在读取过程中实时重算摘要，
// 读到损坏内容会在 Read/Close 时返回 ErrCorrupt（可被 errors.Is 识别）。
func (s *Store) Get(key string) (io.ReadCloser, Version, error) {
	return s.GetVersion(key, -1)
}

// GetVersion 读取键的指定历史版本；idx 为 -1 表示最新版本。
//
// 失败原因可区分：
//   - ErrNotFound：键不存在；
//   - ErrVersionNotFound：键存在但该版本号超出范围；
//   - ErrDataMissing：元数据引用的数据块在磁盘上缺失；
//   - ErrCorrupt：数据块大小或摘要与地址不符。
func (s *Store) GetVersion(key string, idx int) (io.ReadCloser, Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m, err := s.loadMeta(key)
	if err != nil {
		return nil, Version{}, err
	}
	if idx == -1 {
		idx = len(m.Versions) - 1
	}
	if idx < 0 || idx >= len(m.Versions) {
		return nil, Version{}, fmt.Errorf("%w: %q version %d", ErrVersionNotFound, key, idx)
	}
	v := m.toVersion(idx)

	f, err := openObject(s.root, v.Hash, v.Size)
	if err != nil {
		return nil, Version{}, err
	}
	return &verifyingReader{
		f:            f,
		h:            sha256.New(),
		expected:     v.Hash,
		expectedSize: v.Size,
	}, v, nil
}

// openObject 打开数据块并在读取前做“存在性 + 大小”的快速判定，
// 使“缺失”与“损坏”成为两种不同错误。
func openObject(root, addr string, expectedSize int64) (*os.File, error) {
	p := objectPath(root, addr)
	fi, err := os.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: object %s", ErrDataMissing, addr)
		}
		return nil, fmt.Errorf("cas: stat object %s: %w", addr, err)
	}
	if fi.Size() != expectedSize {
		return nil, fmt.Errorf("%w: object %s size: meta=%d disk=%d",
			ErrCorrupt, addr, expectedSize, fi.Size())
	}
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: object %s", ErrDataMissing, addr)
		}
		return nil, fmt.Errorf("cas: open object %s: %w", addr, err)
	}
	return f, nil
}

// verifyingReader 在流式读取时重算 SHA-256，读到 EOF 时与地址比对，
// 因此无需把整个对象读进内存即可发现中途被篡改/损坏的数据。
type verifyingReader struct {
	f            io.ReadCloser
	h            hashState
	expected     string
	expectedSize int64
	readBytes    int64
	verified     bool
	closed       bool
}

// hashState 是流式摘要器的最小接口（*sha256.digest 天然满足）。
type hashState interface {
	io.Writer
	Sum(b []byte) []byte
}

func (r *verifyingReader) Read(p []byte) (int, error) {
	n, err := r.f.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
		r.readBytes += int64(n)
	}
	if errors.Is(err, io.EOF) {
		if verifyErr := r.verify(); verifyErr != nil {
			return n, verifyErr
		}
	}
	return n, err
}

// verify 在首次读到 EOF 时比对字节数与摘要。
func (r *verifyingReader) verify() error {
	if r.verified {
		return nil
	}
	r.verified = true
	if r.readBytes != r.expectedSize {
		return fmt.Errorf("%w: object %s size: meta=%d read=%d",
			ErrCorrupt, r.expected, r.expectedSize, r.readBytes)
	}
	if got := hex.EncodeToString(r.h.Sum(nil)); got != r.expected {
		return fmt.Errorf("%w: object %s checksum: expected=%s actual=%s",
			ErrCorrupt, r.expected, r.expected, got)
	}
	return nil
}

// Close 关闭底层文件；若调用方未读到 EOF，关闭时补做一次完整性判定。
func (r *verifyingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	cerr := r.f.Close()
	if !r.verified {
		if verr := r.verify(); verr != nil {
			return verr
		}
	}
	return cerr
}

// Versions 返回键的全部版本（序号升序）。键不存在返回 ErrNotFound。
func (s *Store) Versions(key string) ([]Version, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m, err := s.loadMeta(key)
	if err != nil {
		return nil, err
	}
	out := make([]Version, len(m.Versions))
	for i := range m.Versions {
		out[i] = m.toVersion(i)
	}
	return out, nil
}
