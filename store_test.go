package cas

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

// ---- 测试辅助 ----

func openTempStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open 失败: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// readAll 读完全部字节（读到 EOF 会触发完整性校验）。
func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("读取对象失败: %v", err)
	}
	return b
}

// countObjectFiles 统计磁盘上实际的数据块文件数（去重的判定依据）。
func countObjectFiles(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	root := filepath.Join(s.root, objectsDir)
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("遍历 objects 目录失败: %v", err)
	}
	return n
}

func requireErrorIs(t *testing.T, err error, target error, basis string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("判定依据[%s]: 期望错误包含 %v，实际 err=%v", basis, target, err)
	}
	t.Logf("判定依据[%s]: 如预期得到 %v（err=%v）", basis, target, err)
}

// ---- 1. 去重：相同内容只存一份 ----

func TestPutGet_Deduplication(t *testing.T) {
	s := openTempStore(t)
	content := []byte("重复写入的数据块内容-v1")

	t.Logf("输入: 相同内容 %q 分别写入两个键，并对同一键再写一次", string(content))
	v1, err := s.Put("key-a", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put key-a 失败: %v", err)
	}
	v2, err := s.Put("key-b", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put key-b 失败: %v", err)
	}
	v1Again, err := s.Put("key-a", bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Put key-a 第二次失败: %v", err)
	}

	if v1.Hash != v2.Hash || v1.Hash != v1Again.Hash {
		t.Fatalf("相同内容应得到相同地址: %s / %s / %s", v1.Hash, v2.Hash, v1Again.Hash)
	}
	t.Logf("判定依据: 三次写入地址均为 %s（内容寻址 ⇒ 相同内容同址）", v1.Hash)

	n := countObjectFiles(t, s)
	if n != 1 {
		t.Fatalf("判定依据: 物理数据块应为 1 份，实际 %d", n)
	}
	t.Logf("判定依据: objects 目录实际文件数=%d，去重成立", n)

	got := readAll(t, mustGet(t, s, "key-a"))
	if !bytes.Equal(got, content) {
		t.Fatalf("读回内容不一致: got=%q want=%q", got, content)
	}
	t.Logf("判定依据: 读回 %q 与输入一致，且读取时通过摘要校验", string(got))
}

func mustGet(t *testing.T, s *Store, key string) io.ReadCloser {
	t.Helper()
	rc, _, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get %q 失败: %v", key, err)
	}
	return rc
}

// ---- 2. 多版本：覆盖写产生新版本，历史版本仍可读 ----

func TestVersions_HistoryReadable(t *testing.T) {
	s := openTempStore(t)
	key := "doc"
	contents := [][]byte{[]byte("v0"), []byte("版本一内容"), bytes.Repeat([]byte("Z"), 4096)}

	for i, c := range contents {
		v, err := s.Put(key, bytes.NewReader(c))
		if err != nil {
			t.Fatalf("Put 第 %d 版失败: %v", i, err)
		}
		t.Logf("输入: 第 %d 次覆盖写 %d 字节 ⇒ 版本序号=%d 地址=%s", i, len(c), v.Index, v.Hash)
	}

	vs, err := s.Versions(key)
	if err != nil {
		t.Fatalf("Versions 失败: %v", err)
	}
	if len(vs) != 3 {
		t.Fatalf("判定依据: 应有 3 个版本，实际 %d", len(vs))
	}
	seen := map[string]bool{}
	for i, v := range vs {
		if v.Index != i {
			t.Fatalf("版本序号应连续升序: 位置 %d 的 Index=%d", i, v.Index)
		}
		seen[v.Hash] = true
	}
	if len(seen) != 3 {
		t.Fatalf("判定依据: 三个不同内容应对应三个不同地址，实际 %d", len(seen))
	}
	t.Logf("判定依据: 版本链长度=%d，三个地址互不相同", len(seen))

	for i, want := range contents {
		rc, v, err := s.GetVersion(key, i)
		if err != nil {
			t.Fatalf("GetVersion(%d) 失败: %v", i, err)
		}
		got := readAll(t, rc)
		if !bytes.Equal(got, want) {
			t.Fatalf("历史版本 %d 内容被改动: got=%d 字节 want=%d 字节", i, len(got), len(want))
		}
		t.Logf("判定依据: 历史版本 %d（地址 %s）读出 %d 字节且校验通过", i, v.Hash, len(got))
	}

	rc, latest, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get 最新版失败: %v", err)
	}
	if latest.Index != 2 || !bytes.Equal(readAll(t, rc), contents[2]) {
		t.Fatalf("默认读取应为最新版本，实际 Index=%d", latest.Index)
	}
	t.Logf("判定依据: Get 默认返回 Index=2 的最新版本")
}

// ---- 3. 缺失 vs 损坏：必须是可区分的错误 ----

func TestRead_MissingVsCorrupt(t *testing.T) {
	s := openTempStore(t)
	content := []byte("ABCDEFG-损坏与缺失区分测试")
	v, err := s.Put("obj", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}

	// 3.1 键不存在 ⇒ ErrNotFound
	_, _, err = s.Get("not-exist")
	requireErrorIs(t, err, ErrNotFound, "键不存在")

	// 3.2 版本号越界 ⇒ ErrVersionNotFound
	_, _, err = s.GetVersion("obj", 99)
	requireErrorIs(t, err, ErrVersionNotFound, "版本号越界")

	// 3.3 数据块文件被删除 ⇒ ErrDataMissing（缺失）
	p := objectPath(s.root, v.Hash)
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 删除数据块文件 %s（模拟丢失）", p)
	_, _, err = s.Get("obj")
	requireErrorIs(t, err, ErrDataMissing, "数据块文件缺失")

	// 3.4 重新写入后篡改内容（等长篡改）⇒ 读取到 EOF 时 ErrCorrupt
	v, err = s.Put("obj", bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	p = objectPath(s.root, v.Hash)
	tampered := append([]byte(nil), content...)
	tampered[0] = 'X' // 等长翻转一个字节，大小检查无法发现，必须靠摘要
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 等长篡改数据块首字节 %q→%q（%s）", content[0], tampered[0], p)
	rc, _, err := s.Get("obj")
	if err != nil {
		t.Fatalf("打开时不应报错，损坏应在读取校验时暴露: %v", err)
	}
	_, err = io.ReadAll(rc)
	_ = rc.Close()
	requireErrorIs(t, err, ErrCorrupt, "等长篡改导致摘要不符")

	// 3.5 截断（大小不符）⇒ 打开时即可判定 ErrCorrupt
	if err := os.WriteFile(p, content[:len(content)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 将数据块截断为 %d/%d 字节（模拟长度损坏）", len(content)/2, len(content))
	_, _, err = s.Get("obj")
	requireErrorIs(t, err, ErrCorrupt, "大小与元数据不符")
}

// ---- 4. 并发写入：外界永远看不到半成品，全部版本可校验 ----

func TestConcurrentPut(t *testing.T) {
	s := openTempStore(t)
	const writers = 16
	const perWriter = 20

	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				payload := fmt.Sprintf("writer=%d seq=%d", w, i)
				if _, err := s.Put("hot-key", bytes.NewReader([]byte(payload))); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	// 并发读者：写入期间持续读取，任何一次成功读取都必须通过完整性校验。
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() {
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				if rc, _, err := s.Get("hot-key"); err == nil {
					b, rerr := io.ReadAll(rc)
					_ = rc.Close()
					if rerr != nil {
						// 正常情况下读到的版本必须完好；ErrNotFound 仅在首个版本
						// 提交前的极短窗口允许（本测试写远快于首个读，实际不会出现）。
						if !errors.Is(rerr, ErrNotFound) {
							errs <- fmt.Errorf("并发读到损坏对象 %q: %w", b, rerr)
						}
					}
				}
				runtime.Gosched()
			}
		}
	}()
	wg.Wait()
	close(stop)
	readerWG.Wait()
	close(errs)
	for e := range errs {
		t.Fatalf("并发写/读异常: %v", e)
	}

	vs, err := s.Versions("hot-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(vs) != writers*perWriter {
		t.Fatalf("判定依据: 应有 %d 个版本（每次 Put 必追加），实际 %d",
			writers*perWriter, len(vs))
	}
	t.Logf("判定依据: %d 个 goroutine × %d 次写共产生 %d 个连续版本，无一丢失",
		writers, perWriter, len(vs))

	// 每个版本都必须能完整、正确读回（不存在被半成品覆盖的窗口）。
	bad := 0
	for i := range vs {
		rc, v, err := s.GetVersion("hot-key", i)
		if err != nil {
			t.Fatalf("版本 %d 打开失败: %v", i, err)
		}
		if _, err := io.Copy(io.Discard, rc); err != nil {
			bad++
			t.Errorf("版本 %d(%s) 校验失败: %v", i, v.Hash, err)
		}
		_ = rc.Close()
	}
	if bad == 0 {
		t.Logf("判定依据: 全部 %d 个历史版本逐一通过摘要校验，无半成品可见", len(vs))
	}
}

// ---- 5. 中断写入：不得发布半成品，旧版本不受影响 ----

type failingReader struct{ remaining int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	n := min(len(p), r.remaining)
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	return n, nil
}

func TestPut_Interrupted(t *testing.T) {
	s := openTempStore(t)
	old := []byte("old-stable-version")
	if _, err := s.Put("k", bytes.NewReader(old)); err != nil {
		t.Fatal(err)
	}

	t.Logf("输入: 已有稳定版本 %q，随后一次写入在传输中途返回 ErrUnexpectedEOF", string(old))
	_, err := s.Put("k", &failingReader{remaining: 5000})
	requireErrorIs(t, err, io.ErrUnexpectedEOF, "写入被中断")

	rc, v, gerr := s.Get("k")
	if gerr != nil {
		t.Fatalf("中断后旧版本应仍可读: %v", gerr)
	}
	got := readAll(t, rc)
	if !bytes.Equal(got, old) {
		t.Fatalf("判定依据: 旧版本被半成品污染: got=%q", got)
	}
	t.Logf("判定依据: 中断后读到的仍是旧版本 Index=%d，内容未变", v.Index)

	vs, _ := s.Versions("k")
	if len(vs) != 1 {
		t.Fatalf("判定依据: 失败的写入不得追加版本，实际版本数=%d", len(vs))
	}
	t.Logf("判定依据: 版本链长度仍为 %d，临时文件留在 tmp/ 等待 GC 清理", len(vs))
}
