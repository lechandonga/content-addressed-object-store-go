package store

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// GCStats 汇总一次垃圾回收的结果。
type GCStats struct {
	// BlobsScanned 扫描到的数据块总数。
	BlobsScanned int
	// BlobsLive 被至少一个版本引用、必须保留的数据块数。
	BlobsLive int
	// BlobsRemoved 被删除的无引用数据块数。
	BlobsRemoved int
	// TempRemoved 被清理的遗留临时文件数（写入崩溃后的残留物）。
	TempRemoved int
	// BytesReclaimed 回收的字节数。
	BytesReclaimed int64
}

// GC 删除不被任何对象版本引用的数据块，并清理遗留临时文件。
//
// 规则：
//   - 只有"没有任何对象的任何一个版本（含历史版本）引用"的数据块才会被删除；
//   - 同一份内容在多个对象/版本间共享时，引用集合做去重，只统计一次、最多删除一次；
//   - GC 持独占访问锁，与进程内外的所有读写互斥，因此不会误删正在被引用或
//     正在发布的数据；
//   - 任一对象的元数据损坏时本次回收整体中止，宁可遗漏也不误删。
func (s *Store) GC(ctx context.Context) (GCStats, error) {
	if err := s.checkWritable("gc"); err != nil {
		return GCStats{}, err
	}
	lk, err := s.beginGC()
	if err != nil {
		return GCStats{}, err
	}
	defer s.endGC(lk)

	var stats GCStats

	// 1) 汇总所有版本引用（含全部历史版本）。
	live, err := s.collectLiveRefs()
	if err != nil {
		return stats, err
	}
	stats.BlobsLive = len(live)

	// 2) 清理写入中断遗留的临时文件（GC 持独占锁，tmp 下不可能有进行中的写入）。
	removed, err := s.cleanTemp()
	if err != nil {
		return stats, err
	}
	stats.TempRemoved = removed

	// 3) 扫描 blobs 树，删除没有任何引用的数据块。
	err = s.sweepBlobs(live, &stats)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

// collectLiveRefs 遍历所有对象目录，汇总全部版本引用的数据块键集合。
// 元数据损坏立即中止（返回 KindCorrupt），调用方据此放弃本次回收。
func (s *Store) collectLiveRefs() (map[ContentKey]struct{}, error) {
	live := make(map[ContentKey]struct{})
	root := filepath.Join(s.root, dirObjects)
	entries, err := os.ReadDir(root)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return live, nil
		}
		return nil, wrapError(KindIO, err, "list objects dir")
	}
	for _, objDir := range entries {
		if !objDir.IsDir() {
			continue
		}
		p := filepath.Join(root, objDir.Name(), fileMeta)
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			if errors.Is(rerr, fs.ErrNotExist) {
				continue // 对象目录新建但尚未发布 meta：保守跳过，不影响删除判定
			}
			return nil, wrapError(KindIO, rerr, "read meta in %q", objDir.Name())
		}
		var st objectState
		if jerr := jsonUnmarshal(data, &st); jerr != nil {
			return nil, wrapError(KindCorrupt, jerr, "object meta %q is corrupt, GC aborted", objDir.Name())
		}
		for _, v := range st.Versions {
			if v.Key.valid() {
				live[v.Key] = struct{}{}
			}
		}
	}
	return live, nil
}

// cleanTemp 清空 tmp 目录中的遗留文件。
func (s *Store) cleanTemp() (int, error) {
	root := filepath.Join(s.root, dirTmp)
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, wrapError(KindIO, err, "list tmp dir")
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(root, e.Name())); err != nil {
			return n, wrapError(KindIO, err, "remove leftover temp %q", e.Name())
		}
		n++
	}
	return n, nil
}

// sweepBlobs 遍历 blobs 树删除无引用数据块，并尝试清理空分片目录。
func (s *Store) sweepBlobs(live map[ContentKey]struct{}, stats *GCStats) error {
	root := filepath.Join(s.root, dirBlobs)
	shardDirs := make(map[string]struct{})
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if p != root {
				shardDirs[p] = struct{}{}
			}
			return nil
		}
		key := ContentKey(d.Name())
		if !key.valid() || !isBlobPath(root, p, key) {
			return nil // 非本存储的数据块文件，保守保留
		}
		stats.BlobsScanned++
		if _, ok := live[key]; ok {
			return nil
		}
		info, ierr := d.Info()
		if ierr != nil {
			return ierr
		}
		if err := os.Remove(p); err != nil {
			return wrapError(KindIO, err, "remove unreferenced blob %s", key)
		}
		stats.BlobsRemoved++
		stats.BytesReclaimed += info.Size()
		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return wrapError(KindIO, err, "sweep blobs")
	}
	// 删除空的分片目录（自下而上）。
	var dirs []string
	for d := range shardDirs {
		dirs = append(dirs, d)
	}
	sortPaths(dirs)
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i]) // 非空目录会失败，忽略即可
	}
	return nil
}

// isBlobPath 确认文件恰好位于 blobs/ab/cd/<64hex> 结构中。
func isBlobPath(blobsRoot, p string, key ContentKey) bool {
	rel, err := filepath.Rel(blobsRoot, p)
	if err != nil {
		return false
	}
	rel = filepath.ToSlash(rel)
	return rel == filepath.ToSlash(key.shardPath())
}
