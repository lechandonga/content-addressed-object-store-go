package cas

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---- 6. GC 基本规则：覆盖后旧内容成为垃圾；历史版本引用的内容保留 ----

func TestGC_ReclaimsUnreferenced(t *testing.T) {
	s := openTempStore(t)

	v0, _ := s.Put("k", bytes.NewReader([]byte("content-version-0")))
	v1, _ := s.Put("k", bytes.NewReader([]byte("content-version-1")))
	v2, _ := s.Put("k", bytes.NewReader([]byte("content-version-2")))

	t.Logf("输入: 键 k 连续三版，地址分别 %s / %s / %s", v0.Hash[:12], v1.Hash[:12], v2.Hash[:12])
	if n := countObjectFiles(t, s); n != 3 {
		t.Fatalf("三个不同内容应产生 3 个数据块，实际 %d", n)
	}

	// 历史版本仍在 ⇒ 三个块都被引用，不能回收。
	st, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	if st.ObjectsRemoved != 0 || st.ObjectsKept != 3 {
		t.Fatalf("判定依据: 历史版本未删除时不应回收，got removed=%d kept=%d",
			st.ObjectsRemoved, st.ObjectsKept)
	}
	t.Logf("判定依据: 首次 GC scanned=%d kept=%d removed=%d（全部版本仍引用）",
		st.ObjectsScanned, st.ObjectsKept, st.ObjectsRemoved)

	// 物理删除键元数据，模拟“版本链不再存在”（测试用后门：直接删文件）。
	if err := os.Remove(metaPath(s.root, "k")); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 删除 k 的元数据（三个版本引用全部消失）")

	st, err = s.GC()
	if err != nil {
		t.Fatal(err)
	}
	if st.ObjectsRemoved != 3 || st.ObjectsKept != 0 {
		t.Fatalf("判定依据: 无任何引用的 3 个块应全部回收，got removed=%d kept=%d",
			st.ObjectsRemoved, st.ObjectsKept)
	}
	t.Logf("判定依据: GC scanned=%d kept=%d removed=%d bytes=%d",
		st.ObjectsScanned, st.ObjectsKept, st.ObjectsRemoved, st.BytesRemoved)

	// 幂等：再跑一次没有垃圾可删，也不会重复释放。
	st2, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	if st2.ObjectsRemoved != 0 {
		t.Fatalf("判定依据: 重复 GC 必须幂等，第二次 removed=%d", st2.ObjectsRemoved)
	}
	t.Logf("判定依据: 再次 GC removed=%d，无重复释放", st2.ObjectsRemoved)
}

// ---- 7. 共享内容：多键去重的块，只要还有一个引用就必须保留 ----

func TestGC_SharedContent(t *testing.T) {
	s := openTempStore(t)
	shared := []byte("多个键共享的同一份内容")
	a, _ := s.Put("a", bytes.NewReader(shared))
	b, _ := s.Put("b", bytes.NewReader(shared))
	c, _ := s.Put("c", bytes.NewReader(shared))

	t.Logf("输入: 键 a/b/c 写入相同内容，版本地址均为 %s", a.Hash)
	if a.Hash != b.Hash || b.Hash != c.Hash {
		t.Fatal("相同内容地址应一致")
	}
	if n := countObjectFiles(t, s); n != 1 {
		t.Fatalf("判定依据: 物理块应为 1，实际 %d", n)
	}

	if err := os.Remove(metaPath(s.root, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(metaPath(s.root, "b")); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 删除 a、b 的引用，c 仍引用该块")

	st, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	if st.ObjectsRemoved != 0 {
		t.Fatalf("判定依据: 仍被 c 引用的共享块不得删除，removed=%d", st.ObjectsRemoved)
	}
	t.Logf("判定依据: GC kept=%d removed=%d，剩余引用保护了共享块",
		st.ObjectsKept, st.ObjectsRemoved)

	if _, _, err := s.Get("c"); err != nil {
		t.Fatalf("判定依据: GC 后 c 必须仍可正常读取: %v", err)
	}
	t.Logf("判定依据: GC 后读取 c 成功并通过校验（未误删）")
}

// ---- 8. GC 清理崩溃残留的临时文件（但只清理“陈旧”的） ----

func TestGC_StaleTempCleanup(t *testing.T) {
	s := openTempStore(t)
	stale := filepath.Join(s.root, tmpDir, "obj-stale.tmp")
	fresh := filepath.Join(s.root, tmpDir, "obj-fresh.tmp")
	if err := os.WriteFile(stale, []byte("crash leftover"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fresh, []byte("in-flight"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * gcStaleTempAge)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: tmp/ 中放置 2 小时前的残留 %s 与刚创建的 %s",
		filepath.Base(stale), filepath.Base(fresh))

	st, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	if st.TempFilesRemoved != 1 {
		t.Fatalf("判定依据: 应只清理 1 个陈旧临时文件，实际 %d", st.TempFilesRemoved)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("判定依据: 陈旧临时文件应被删除")
	}
	if _, err := os.Stat(fresh); err != nil {
		t.Fatalf("判定依据: 新鲜临时文件必须保留（可能是正在进行的写）: %v", err)
	}
	t.Logf("判定依据: 陈旧残留已删，新鲜临时文件保留，tmp_removed=%d", st.TempFilesRemoved)
}

// ---- 9. GC 与并发读写：不得误删引用块，不得出现缺失/重复释放 ----

func TestGC_ConcurrentWithReadWrite(t *testing.T) {
	s := openTempStore(t)
	// 预置一批键与内容，使 GC 在整个运行期间始终面对“存活集合”。
	keys := make([]string, 20)
	payloads := make(map[string][]byte)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
		c := []byte(fmt.Sprintf("persistent-content-of-%02d", i))
		payloads[keys[i]] = c
		if _, err := s.Put(keys[i], bytes.NewReader(c)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("输入: 预置 %d 个持久键作为不可回收的存活集合", len(keys))

	const rounds = 30
	var wg sync.WaitGroup
	errCh := make(chan error, 32)

	// GC 方：反复回收。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			st, err := s.GC()
			if err != nil {
				errCh <- fmt.Errorf("GC 第 %d 轮: %w", i, err)
				return
			}
			// ephemeral 版本在 GC 期间持续增长，存活数是动态的；
			// 关键判定：removed 必须恒为 0（所有块都被某版本引用），
			// 且至少 20 个持久键的块在每轮存活集合中。
			if st.ObjectsRemoved != 0 || st.ObjectsKept < len(keys) {
				errCh <- fmt.Errorf("第 %d 轮 GC 判定异常: removed=%d kept=%d scanned=%d",
					i, st.ObjectsRemoved, st.ObjectsKept, st.ObjectsScanned)
				return
			}
		}
	}()

	// 读方：持续读取全部持久键，每次都必须完整通过校验。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds*5; i++ {
			for _, k := range keys {
				rc, _, err := s.Get(k)
				if err != nil {
					errCh <- fmt.Errorf("读 %s: %w", k, err)
					return
				}
				got, rerr := io.ReadAll(rc)
				_ = rc.Close()
				if rerr != nil {
					errCh <- fmt.Errorf("校验 %s: %w", k, rerr)
					return
				}
				if !bytes.Equal(got, payloads[k]) {
					errCh <- fmt.Errorf("键 %s 内容被中途改动", k)
					return
				}
			}
		}
	}()

	// 写方：对临时键反复覆盖写，制造“垃圾产生与 GC 赛跑”。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds*10; i++ {
			c := []byte(fmt.Sprintf("ephemeral-%d-payload", i))
			if _, err := s.Put("ephemeral", bytes.NewReader(c)); err != nil {
				errCh <- fmt.Errorf("写 ephemeral 第 %d 次: %w", i, err)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Error(e)
	}

	// 收尾：ephemeral 的历史版本仍全部被元数据引用，GC 不应删除任何块；
	// 验证“存活集合”判定在最终状态下依旧自洽。
	st, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	vs, _ := s.Versions("ephemeral")
	if st.ObjectsRemoved != 0 {
		t.Fatalf("判定依据: 所有版本仍被引用，收尾 GC 不应删除任何块，removed=%d", st.ObjectsRemoved)
	}
	t.Logf("判定依据: 并发结束后 %d 轮 GC 零误删；ephemeral 共 %d 版，数据块全部可读",
		rounds, len(vs))
}

// ---- 10. 高频 写/GC 赛跑：GC 不得删除刚提交版本引用的块 ----
//
// 对应修复过的竞争：块“改名发布”与“元数据提交”之间若被 GC 插入，
// 旧实现会留下悬挂引用（ErrDataMissing）。写锁内采集存活集合后，
// 所有提交完成的版本引用的块必须始终完好。
func TestGC_NoDanglingReferenceUnderRace(t *testing.T) {
	s := openTempStore(t)

	const writers, puts, gcRounds = 8, 60, 80
	var wg sync.WaitGroup
	errCh := make(chan error, 8)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < puts; i++ {
				c := []byte(fmt.Sprintf("writer-%d-seq-%03d-xxxxxxxxxx", w, i))
				if _, err := s.Put("race-key", bytes.NewReader(c)); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < gcRounds; i++ {
			if _, err := s.GC(); err != nil {
				errCh <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for e := range errCh {
		t.Fatal(e)
	}

	vs, err := s.Versions("race-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != writers*puts {
		t.Fatalf("判定依据: 版本数应为 %d，实际 %d", writers*puts, len(vs))
	}
	missing, corrupt, okN := 0, 0, 0
	for i := range vs {
		rc, _, err := s.GetVersion("race-key", i)
		if err != nil {
			switch {
			case errors.Is(err, ErrDataMissing):
				missing++
			case errors.Is(err, ErrCorrupt):
				corrupt++
			default:
				t.Fatalf("版本 %d 意外错误: %v", i, err)
			}
			continue
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			corrupt++
		} else {
			okN++
		}
		_ = rc.Close()
	}
	t.Logf("输入: %d 写者 × %d 次写 与 %d 轮 GC 并发；判定依据: %d 个版本中 完好=%d 缺失=%d 损坏=%d",
		writers, puts, gcRounds, len(vs), okN, missing, corrupt)
	if missing != 0 || corrupt != 0 {
		t.Fatalf("判定依据: 不允许出现缺失/损坏版本：缺失=%d 损坏=%d", missing, corrupt)
	}
}
