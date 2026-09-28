package cas

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GCStats 汇报一次垃圾回收的判定与执行结果，便于解释与测试。
type GCStats struct {
	ObjectsScanned   int   `json:"objects_scanned"`    // 磁盘上扫描到的数据块数量
	ObjectsKept      int   `json:"objects_kept"`       // 仍被某个版本引用而保留的数据块
	ObjectsRemoved   int   `json:"objects_removed"`    // 确认无引用后删除的数据块
	BytesRemoved     int64 `json:"bytes_removed"`      // 删除释放的字节数
	TempFilesRemoved int   `json:"temp_files_removed"` // 清理的中断写入残留临时文件数
}

// gcStaleTempAge 超过该年龄的 tmp 文件视为崩溃残留。
const gcStaleTempAge = time.Hour

// GC 回收不再被任何版本引用的数据块。
//
// 判定依据：
//  1. 先在写锁保护下扫描全部键的元数据，汇总“存活地址集合”——
//     只要数据块被任意键的任意版本（含历史版本）引用就属于存活；
//  2. 保持写锁扫描 objects/ 目录，逐个用地址在存活集合中判定，
//     不在集合中的才删除。
//
// 为什么存活集合必须在写锁内采集：Put 全程持写锁，其“数据块改名 →
// 元数据提交”两步不可能在 GC 持写锁期间交错进行。因此 GC 看到的状态
// 要么已包含新版本（块保留），要么 Put 根本未开始（块确实是垃圾），
// 不存在“块刚发布、元数据尚未提交而被误删”的中间态。
//
// 写锁与 Put/Get 互斥，因此：
//   - 不会误删仍被引用的数据；
//   - 每个地址只判定、删除一次，不会重复释放；
//   - 数据块按地址唯一命名，重跑 GC 对同一批垃圾幂等。
func (s *Store) GC() (GCStats, error) {
	var st GCStats

	// 独占写锁：与所有 Put/Get 串行化，保证存活集合与删除判定的一致性。
	s.mu.Lock()
	defer s.mu.Unlock()

	live, err := s.collectLive()
	if err != nil {
		return st, err
	}

	objsRoot := filepath.Join(s.root, objectsDir)
	entries, err := os.ReadDir(objsRoot)
	if err != nil {
		return st, err
	}
	for _, shard := range entries {
		if !shard.IsDir() || len(shard.Name()) != 2 || !isHex(shard.Name()) {
			continue
		}
		files, err := os.ReadDir(filepath.Join(objsRoot, shard.Name()))
		if err != nil {
			return st, err
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			addr := shard.Name() + f.Name()
			if len(addr) != hashHexLen || !isHex(addr) {
				continue // 非本系统数据块，忽略
			}
			st.ObjectsScanned++
			if live[addr] {
				st.ObjectsKept++
				continue
			}
			p := filepath.Join(objsRoot, shard.Name(), f.Name())
			info, err := f.Info()
			if err == nil {
				st.BytesRemoved += info.Size()
			}
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return st, err
			}
			st.ObjectsRemoved++
		}
	}

	st.TempFilesRemoved += s.cleanStaleTempFiles()
	return st, nil
}

// collectLive 扫描 meta/ 下所有键的元数据，汇总被任意版本引用的地址。
// 调用方持有（或即将持有）读锁。
func (s *Store) collectLive() (map[string]bool, error) {
	live := make(map[string]bool)
	metaRoot := filepath.Join(s.root, metaDir)
	entries, err := os.ReadDir(metaRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return live, nil
		}
		return nil, err
	}
	for _, shard := range entries {
		if !shard.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(metaRoot, shard.Name()))
		if err != nil {
			return nil, err
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
				continue
			}
			m, err := s.loadMetaFile(filepath.Join(metaRoot, shard.Name(), f.Name()))
			if err != nil {
				return nil, err
			}
			for _, v := range m.Versions {
				live[v.Hash] = true
			}
		}
	}
	return live, nil
}

// loadMetaFile 从指定路径解码键元数据。
func (s *Store) loadMetaFile(p string) (*keyMeta, error) {
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var m keyMeta
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// cleanStaleTempFiles 删除 tmp/ 中超过 gcStaleTempAge 的残留（中断的 Put）。
// 调用方持有写锁，因此不会碰到正在进行的 Put。
func (s *Store) cleanStaleTempFiles() int {
	removed := 0
	now := time.Now()
	tmpRoot := filepath.Join(s.root, tmpDir)
	entries, err := os.ReadDir(tmpRoot)
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".tmp") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if now.Sub(info.ModTime()) < gcStaleTempAge {
			continue
		}
		if err := os.Remove(filepath.Join(tmpRoot, e.Name())); err == nil {
			removed++
		}
	}
	return removed
}

func isHex(s string) bool {
	for _, c := range []byte(s) {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
