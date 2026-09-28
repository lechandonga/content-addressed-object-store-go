package store

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// validateName 校验对象名。对象目录使用 PathEscape 编码，只有 "." 与 ".."
// 两个名字转义后仍保持原样、可能造成目录穿越，这里显式拒绝。
func validateName(name string) error {
	if name == "" {
		return newError(KindIO, "object name must not be empty")
	}
	if name == "." || name == ".." || strings.ContainsRune(name, 0) {
		return newError(KindIO, "invalid object name %q", name)
	}
	return nil
}

// Put 将 r 中的全部数据写入对象 name，生成一个新版本并成为当前版本。
//
// 过程：
//  1. 流式写入 tmp/ 临时文件，同时计算 SHA-256（外界在 blobs/ 下看不到任何东西）；
//  2. 将临时文件原子 rename 为 blobs/<键分片>，已存在则直接复用（去重，只存一份）；
//  3. 在该对象的提交锁内重读元数据、追加新版本号，meta.json 同样以原子 rename 发布。
//
// 任一步失败或进程崩溃，外界都不会看到半成品对象或半成品数据块；
// 遗留的临时文件由 GC 清理。返回新版本描述。
func (s *Store) Put(ctx context.Context, name string, r io.Reader) (Version, error) {
	if err := validateName(name); err != nil {
		return Version{}, err
	}
	if err := s.checkWritable("put"); err != nil {
		return Version{}, err
	}
	if r == nil {
		return Version{}, newError(KindIO, "nil reader for object %q", name)
	}
	lk, err := s.beginRead()
	if err != nil {
		return Version{}, err
	}
	defer s.endRead(lk)

	tmp, size, key, err := s.writeTempFile(r)
	if err != nil {
		return Version{}, err
	}
	if err := s.commitBlob(tmp, key, size); err != nil {
		return Version{}, err
	}

	var result Version
	commitErr := s.withCommitLock(name, func() error {
		st, rerr := s.readObjectState(name)
		if rerr != nil {
			var nf *NotFoundError
			if !errors.As(rerr, &nf) {
				return rerr
			}
			st = &objectState{}
		}
		var nextID int64 = 1
		if len(st.Versions) > 0 {
			nextID = st.Versions[len(st.Versions)-1].ID + 1
		}
		v := versionMeta{ID: nextID, Key: key, Size: size, CreatedAt: nowUTC()}
		st.Versions = append(st.Versions, v)
		st.Current = nextID
		if err := s.writeMetaAtomic(name, st); err != nil {
			return err
		}
		result = Version(v)
		return nil
	})
	if commitErr != nil {
		return Version{}, commitErr
	}
	// 对象目录若为新建，持久化其父目录项（失败不影响本次写入的可见性）。
	_ = fsyncDir(filepath.Join(s.root, dirObjects))
	return result, nil
}

// openBlob 定位并打开版本指向的数据块。
// 元数据引用的数据块在磁盘上缺失 -> *NotFoundError（与"对象不存在"同属缺失类，
// 但 Obj/Key 信息不同，可据此区分具体缺了什么）。
func (s *Store) openBlob(name string, v versionMeta) (*os.File, error) {
	p := filepath.Join(s.root, dirBlobs, v.Key.shardPath())
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &NotFoundError{Obj: name, Version: v.ID, Key: v.Key}
		}
		return nil, wrapError(KindIO, err, "open blob %s for %q", v.Key, name)
	}
	return f, nil
}

// Get 读取对象 name 的当前版本内容。
// 对象不存在或数据块缺失返回 *NotFoundError；读取完成时校验 SHA-256，
// 不匹配则返回 *CorruptionError，两类原因可被 errors.As 明确区分。
func (s *Store) Get(ctx context.Context, name string) (io.ReadCloser, Version, error) {
	if err := validateName(name); err != nil {
		return nil, Version{}, err
	}
	lk, err := s.beginRead()
	if err != nil {
		return nil, Version{}, err
	}
	st, err := s.readObjectState(name)
	if err != nil {
		s.endRead(lk)
		return nil, Version{}, err
	}
	v, ok := st.find(st.Current)
	if !ok {
		s.endRead(lk)
		return nil, Version{}, &NotFoundError{Obj: name, Version: st.Current}
	}
	rc, err := s.openVersionedBlob(name, v, lk)
	if err != nil {
		s.endRead(lk)
		return nil, Version{}, err
	}
	return rc, Version(v), nil
}

// GetVersion 读取对象 name 的指定历史版本内容。
func (s *Store) GetVersion(ctx context.Context, name string, versionID int64) (io.ReadCloser, Version, error) {
	if err := validateName(name); err != nil {
		return nil, Version{}, err
	}
	if versionID <= 0 {
		return nil, Version{}, newError(KindIO, "version id must be positive, got %d", versionID)
	}
	lk, err := s.beginRead()
	if err != nil {
		return nil, Version{}, err
	}
	st, err := s.readObjectState(name)
	if err != nil {
		var nf *NotFoundError
		if errors.As(err, &nf) {
			s.endRead(lk)
			return nil, Version{}, &NotFoundError{Obj: name, Version: versionID}
		}
		s.endRead(lk)
		return nil, Version{}, err
	}
	v, ok := st.find(versionID)
	if !ok {
		s.endRead(lk)
		return nil, Version{}, &NotFoundError{Obj: name, Version: versionID}
	}
	rc, err := s.openVersionedBlob(name, v, lk)
	if err != nil {
		s.endRead(lk)
		return nil, Version{}, err
	}
	return rc, Version(v), nil
}

// openVersionedBlob 打开数据块并包上读时完整性校验器。
// 共享访问锁 lk 的所有权转移给返回的 ReadCloser，在其 Close 时释放。
func (s *Store) openVersionedBlob(name string, v versionMeta, lk *fileLock) (io.ReadCloser, error) {
	f, err := s.openBlob(name, v)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{
		rc:       f,
		h:        newSha256(),
		want:     v.Key,
		wantSize: v.Size,
		obj:      name,
		version:  v.ID,
		release:  func() { s.endRead(lk) },
	}, nil
}

// Versions 按版本号从新到旧返回对象 name 的全部版本描述。
func (s *Store) Versions(ctx context.Context, name string) ([]Version, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	lk, err := s.beginRead()
	if err != nil {
		return nil, err
	}
	defer s.endRead(lk)
	st, err := s.readObjectState(name)
	if err != nil {
		return nil, err
	}
	out := make([]Version, 0, len(st.Versions))
	for i := len(st.Versions) - 1; i >= 0; i-- {
		out = append(out, Version(st.Versions[i]))
	}
	return out, nil
}

// CurrentVersion 返回对象当前版本号与描述。
func (s *Store) CurrentVersion(ctx context.Context, name string) (Version, error) {
	if err := validateName(name); err != nil {
		return Version{}, err
	}
	lk, err := s.beginRead()
	if err != nil {
		return Version{}, err
	}
	defer s.endRead(lk)
	st, err := s.readObjectState(name)
	if err != nil {
		return Version{}, err
	}
	v, ok := st.find(st.Current)
	if !ok {
		return Version{}, &NotFoundError{Obj: name, Version: st.Current}
	}
	return Version(v), nil
}

// find 在版本链中查找指定版本号。
func (st *objectState) find(id int64) (versionMeta, bool) {
	i := sort.Search(len(st.Versions), func(i int) bool { return st.Versions[i].ID >= id })
	if i < len(st.Versions) && st.Versions[i].ID == id {
		return st.Versions[i], true
	}
	return versionMeta{}, false
}
