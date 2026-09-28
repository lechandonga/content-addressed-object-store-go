package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestGCPreservesReferenced 判定依据：所有被当前或历史版本引用的数据块
// （包括被多个对象/版本共享的同一份内容）在 GC 后必须仍然可读。
func TestGCPreservesReferenced(t *testing.T) {
	s := newTestStore(t)
	shared := "shared-content"
	uniq := "only-in-v1-of-doc"
	t.Logf("input: alpha=%q beta=%q doc 覆盖两版(v1=%q, v2=%q)", shared, shared, uniq, shared)

	mustPut(t, s, "alpha", shared)
	mustPut(t, s, "beta", shared)
	mustPut(t, s, "doc", uniq)
	mustPut(t, s, "doc", shared)

	if n := countBlobs(t, s); n != 2 {
		t.Fatalf("precondition: 2 unique blobs (shared + uniq), got %d", n)
	}
	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	t.Logf("判定依据(1): GC=%+v：活跃块应去重后为 2，删除 0（共享块不因 alpha/beta 重复计数）", stats)
	if stats.BlobsLive != 2 || stats.BlobsRemoved != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	// 当前版本与历史版本都必须完好可读。
	if got := mustGet(t, s, "alpha"); got != shared {
		t.Fatalf("alpha = %q", got)
	}
	if got := mustGet(t, s, "beta"); got != shared {
		t.Fatalf("beta = %q", got)
	}
	if got := mustGetVersion(t, s, "doc", 1); got != uniq {
		t.Fatalf("doc v1 historical content lost: %q", got)
	}
	if got := mustGetVersion(t, s, "doc", 2); got != shared {
		t.Fatalf("doc v2 = %q", got)
	}
}

// seedOrphanBlob 直接在 blobs/ 下放入一个无引用的数据块，
// 模拟"数据块已发布但元数据提交前崩溃"的残留物。
func seedOrphanBlob(t *testing.T, s *Store, content string) ContentKey {
	t.Helper()
	f, _, k, err := s.writeTempFile(strings.NewReader(content))
	if err != nil {
		t.Fatalf("seed temp: %v", err)
	}
	if err := s.commitBlob(f, k, int64(len(content))); err != nil {
		t.Fatalf("seed orphan blob: %v", err)
	}
	return k
}

// TestGCRemovesOrphansOnce 判定依据：无引用块被删除且只删除一次；
// 第二次 GC 不应重复释放。
func TestGCRemovesOrphansOnce(t *testing.T) {
	s := newTestStore(t)
	v := mustPut(t, s, "keep", "alive-content")
	k1 := seedOrphanBlob(t, s, "orphan-one")
	k2 := seedOrphanBlob(t, s, "orphan-two")
	t.Logf("input: 1 个存活块 %s 与 2 个无引用块 %s,%s", v.Key, k1, k2)

	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("gc1: %v", err)
	}
	t.Logf("判定依据(1): 首次 GC 应扫描 3 块、保留 1、删除 2，实际 %+v", stats)
	if stats.BlobsScanned != 3 || stats.BlobsLive != 1 || stats.BlobsRemoved != 2 {
		t.Fatalf("gc1 stats = %+v", stats)
	}
	if _, err := os.Stat(s.blobFile(v.Key)); err != nil {
		t.Fatalf("live blob must remain: %v", err)
	}
	for _, k := range []ContentKey{k1, k2} {
		if _, err := os.Stat(s.blobFile(k)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("orphan %s should have been removed, stat err=%v", k, err)
		}
	}
	stats2, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("gc2: %v", err)
	}
	t.Logf("判定依据(2): 第二次 GC 不应重复释放，实际 删除=%d 回收字节=%d", stats2.BlobsRemoved, stats2.BytesReclaimed)
	if stats2.BlobsRemoved != 0 || stats2.BytesReclaimed != 0 || stats2.BlobsLive != 1 {
		t.Fatalf("gc2 stats = %+v", stats2)
	}
}

// TestGCAbortsOnCorruptMeta 判定依据：任一对象元数据损坏时 GC 整体中止，
// 本次不删除任何数据（宁可遗漏，不可误删），并返回 KindCorrupt。
func TestGCAbortsOnCorruptMeta(t *testing.T) {
	s := newTestStore(t)
	mustPut(t, s, "good", "good-content")
	k := seedOrphanBlob(t, s, "orphan")
	badDir := s.objectDir("bad")
	if err := os.MkdirAll(badDir, dirPerm); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, fileMeta), []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("write corrupt meta: %v", err)
	}
	t.Logf("输入: 对象 bad 的 meta.json 损坏，另有 1 个无引用块 %s", k)

	_, err := s.GC(context.Background())
	var pe *Error
	if !errors.As(err, &pe) || pe.Kind != KindCorrupt {
		t.Fatalf("expected KindCorrupt error, got %T: %v", err, err)
	}
	t.Logf("判定依据(1): GC 返回 KindCorrupt: %v", err)
	if _, statErr := os.Stat(s.blobFile(k)); statErr != nil {
		t.Fatalf("aborted GC must not remove anything, but orphan gone: %v", statErr)
	}
	if got := mustGet(t, s, "good"); got != "good-content" {
		t.Fatalf("good object unreadable after aborted GC: %q", got)
	}
	t.Logf("判定依据(2): 无引用块与正常对象数据均未被改动")
}

// TestReadOnlyRejectsMutations 判定依据：只读模式下 Put/GC 被拒绝。
func TestReadOnlyRejectsMutations(t *testing.T) {
	dir := t.TempDir()
	// 先用读写模式初始化仓库布局，再以只读模式重开。
	init, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	mustPut(t, init, "seed", "seed-data")
	_ = init.Close()

	s, err := Open(dir, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("open ro: %v", err)
	}
	defer s.Close()
	if _, err := s.Put(context.Background(), "x", stringsReader("data")); err == nil {
		t.Fatal("put on read-only store should fail")
	} else {
		t.Logf("判定依据: 只读 Put 被拒绝: %v", err)
	}
	if _, err := s.GC(context.Background()); err == nil {
		t.Fatal("gc on read-only store should fail")
	}
	// 只读模式打开空目录也能读（返回缺失）。
	_, _, gerr := s.Get(context.Background(), "x")
	var nf *NotFoundError
	if !errors.As(gerr, &nf) {
		t.Fatalf("expected NotFoundError on empty ro store, got %T: %v", gerr, gerr)
	}
}
