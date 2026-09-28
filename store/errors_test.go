package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMissingObject 判定依据：读取从不存在的对象，必须得到 *NotFoundError，
// 而不是笼统的 I/O 错误。
func TestMissingObject(t *testing.T) {
	s := newTestStore(t)
	t.Logf("input: GET 从未写入的对象 %q", "ghost")
	_, _, err := s.Get(context.Background(), "ghost")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
	t.Logf("判定依据: 错误类型=%T, 消息=%q", err, err.Error())
	if nf.Obj != "ghost" || nf.Key != "" {
		t.Fatalf("unexpected NotFoundError fields: %+v", nf)
	}

	_, _, err = s.GetVersion(context.Background(), "ghost", 99)
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError for missing version, got %T: %v", err, err)
	}
	if nf.Version != 99 {
		t.Fatalf("missing-version error should carry version 99, got %d", nf.Version)
	}
	t.Logf("判定依据: 缺失版本错误携带 version=%d", nf.Version)
}

// TestMissingBlob 判定依据：对象元数据存在，但其引用的数据块文件被删掉，
// 应得到携带 Key 的 *NotFoundError（说明缺的是数据块而非对象）。
func TestMissingBlob(t *testing.T) {
	s := newTestStore(t)
	v := mustPut(t, s, "doc", "hello world")
	if err := os.Remove(s.blobFile(v.Key)); err != nil {
		t.Fatalf("remove blob: %v", err)
	}
	t.Logf("input: 删除对象 %q 当前版本引用的数据块 %s", "doc", v.Key)

	_, _, err := s.Get(context.Background(), "doc")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError for deleted blob, got %T: %v", err, err)
	}
	t.Logf("判定依据: 错误类型=%T, Key=%s, 消息=%q", err, nf.Key, err.Error())
	if nf.Key != v.Key {
		t.Fatalf("NotFoundError.Key = %s, want %s", nf.Key, v.Key)
	}
}

// TestCorruptBlob 判定依据：数据块文件存在但字节被篡改，读取时重新计算的
// SHA-256 与内容键不一致，必须得到 *CorruptionError，且错误中给出实际摘要。
func TestCorruptBlob(t *testing.T) {
	s := newTestStore(t)
	v := mustPut(t, s, "doc", "important payload")
	p := s.blobFile(v.Key)
	if err := os.WriteFile(p, []byte("tampered!!!"), 0o644); err != nil {
		t.Fatalf("tamper blob: %v", err)
	}
	t.Logf("input: 将数据块 %s 的内容篡改为 %q", v.Key, "tampered!!!")

	rc, _, err := s.Get(context.Background(), "doc")
	if err != nil {
		t.Fatalf("open corrupted blob should succeed until read: %v", err)
	}
	_, rerr := io.ReadAll(rc)
	var ce *CorruptionError
	if !errors.As(rerr, &ce) {
		t.Fatalf("expected *CorruptionError, got %T: %v", rerr, rerr)
	}
	t.Logf("判定依据: 错误类型=%T, 期望键=%s, 实算摘要=%q", rerr, ce.Key, ce.Got)
	if ce.Key != v.Key || ce.Got == "" || ce.Got == string(v.Key) {
		t.Fatalf("corruption error not explained properly: %+v", ce)
	}
	_ = rc.Close()

	// 与"缺失"明确区分：同一场景不应被报告为 NotFound。
	var nf *NotFoundError
	if errors.As(rerr, &nf) {
		t.Fatalf("corruption must not be reported as not-found")
	}
}

// TestTruncatedBlob 判定依据：数据块存在但被截断（长度不足），
// 在 ReadAll 或 Close 时也必须被识别为损坏。
func TestTruncatedBlob(t *testing.T) {
	s := newTestStore(t)
	v := mustPut(t, s, "doc", strings.Repeat("x", 2048))
	p := s.blobFile(v.Key)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if err := os.WriteFile(p, raw[:100], 0o644); err != nil {
		t.Fatalf("truncate blob: %v", err)
	}
	t.Logf("input: 将数据块 %s 从 %d 字节截断为 100 字节", v.Key, len(raw))

	rc, _, err := s.Get(context.Background(), "doc")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, rerr := io.Copy(io.Discard, rc)
	if rerr == nil {
		rerr = rc.Close()
	} else {
		_ = rc.Close()
	}
	var ce *CorruptionError
	if !errors.As(rerr, &ce) {
		t.Fatalf("truncated blob expected *CorruptionError, got %T: %v", rerr, rerr)
	}
	t.Logf("判定依据: 截断被识别为损坏，消息=%q", rerr.Error())
}

// TestPutInterrupted 判定依据：写入数据源在中途失败，Put 返回错误后，
// 对象与数据块均不可见（没有半成品），遗留临时文件可被 GC 清理。
type failingReader struct{ remaining int }

func (f failingReader) Read(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, fmt.Errorf("simulated upstream failure")
	}
	n := len(p)
	if n > f.remaining {
		n = f.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'z'
	}
	f.remaining -= n
	if f.remaining == 0 {
		return n, fmt.Errorf("simulated upstream failure mid-stream")
	}
	return n, nil
}

func TestPutInterrupted(t *testing.T) {
	s := newTestStore(t)
	t.Logf("input: PUT 对象 %q，数据源在传出部分字节后报错", "doc")

	_, err := s.Put(context.Background(), "doc", failingReader{remaining: 4096})
	if err == nil {
		t.Fatal("expected Put to fail when source fails")
	}
	t.Logf("判定依据(1): Put 返回错误=%v；失败写入由代码自行清理，对象 GET 应得到 NotFoundError", err)

	var nf *NotFoundError
	if _, _, gerr := s.Get(context.Background(), "doc"); !errors.As(gerr, &nf) {
		t.Fatalf("after failed put, object must be invisible, got %T: %v", gerr, gerr)
	}
	if n := countBlobs(t, s); n != 0 {
		t.Fatalf("no blob may be published on interruption, found %d", n)
	}

	// 模拟进程在发布数据块后、更新元数据前崩溃：blobs/ 里留下一个无引用块。
	orphan := "orphan-content-never-committed"
	f, _, k, werr := s.writeTempFile(strings.NewReader(orphan))
	if werr != nil {
		t.Fatalf("write orphan temp: %v", werr)
	}
	if err := s.commitBlob(f, k, int64(len(orphan))); err != nil {
		t.Fatalf("commit orphan blob: %v", err)
	}
	// 再造一个 tmp 残留文件，模拟崩溃时未发布的临时文件。
	if err := os.WriteFile(filepath.Join(s.root, dirTmp, "incoming-leftover"), []byte("x"), 0o600); err != nil {
		t.Fatalf("seed leftover temp: %v", err)
	}
	before := countBlobs(t, s)
	t.Logf("判定依据(2): 崩溃后 blobs 中有 %d 个无引用块、tmp 中有 1 个残留文件", before)

	stats, gerr := s.GC(context.Background())
	if gerr != nil {
		t.Fatalf("gc: %v", gerr)
	}
	t.Logf("判定依据(3): GC 结果=%+v：无引用块被删、临时文件被清", stats)
	if stats.BlobsRemoved != 1 || stats.TempRemoved != 1 {
		t.Fatalf("GC stats = %+v, want 1 blob + 1 temp removed", stats)
	}
	if n := countBlobs(t, s); n != 0 {
		t.Fatalf("after GC expected 0 blobs, got %d", n)
	}
}
