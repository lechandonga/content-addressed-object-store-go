package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// ---------- 测试辅助 ----------

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// short 截断过长的输入，保证单测日志可读，同时仍能看到判定依据。
func short(s string) string {
	const limit = 80
	if len(s) <= limit {
		return strconv.Quote(s)
	}
	return strconv.Quote(s[:limit]) + fmt.Sprintf("...(共 %d 字节)", len(s))
}

func mustPut(t *testing.T, s *Store, name, data string) Version {
	t.Helper()
	v, err := s.Put(context.Background(), name, strings.NewReader(data))
	if err != nil {
		t.Fatalf("put %q: %v", name, err)
	}
	t.Logf("PUT object=%q input=%s -> version=%d key=%s size=%d", name, short(data), v.ID, v.Key, v.Size)
	return v
}

func mustGet(t *testing.T, s *Store, name string) string {
	t.Helper()
	rc, v, err := s.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("get %q: %v", name, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q: %v", name, err)
	}
	t.Logf("GET object=%q -> version=%d key=%s content=%s", name, v.ID, v.Key, short(string(b)))
	return string(b)
}

func mustGetVersion(t *testing.T, s *Store, name string, id int64) string {
	t.Helper()
	rc, v, err := s.GetVersion(context.Background(), name, id)
	if err != nil {
		t.Fatalf("get %q v%d: %v", name, id, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %q v%d: %v", name, id, err)
	}
	t.Logf("GET object=%q version=%d -> key=%s content=%s", name, id, v.Key, short(string(b)))
	return string(b)
}

func (s *Store) blobFile(k ContentKey) string {
	return filepath.Join(s.root, dirBlobs, k.shardPath())
}

func countBlobs(t *testing.T, s *Store) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(s.root, dirBlobs), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk blobs: %v", err)
	}
	return n
}

// ---------- 用例 ----------

// TestDedup 验证：内容相同的数据自动合并，磁盘上只存一份。
func TestDedup(t *testing.T) {
	s := newTestStore(t)
	content := "the quick brown fox jumps over the lazy dog"
	t.Logf("input: 两个不同对象名写入完全相同的内容 %q", content)

	v1 := mustPut(t, s, "alpha", content)
	v2 := mustPut(t, s, "beta", content)

	t.Logf("判定依据: 两次写入的内容键应相等(%s == %s)，且 blobs 目录只有 1 个数据块", v1.Key, v2.Key)
	if v1.Key != v2.Key {
		t.Fatalf("dedup key mismatch: %s != %s", v1.Key, v2.Key)
	}
	if n := countBlobs(t, s); n != 1 {
		t.Fatalf("expected 1 blob after dedup, got %d", n)
	}
	if got := mustGet(t, s, "alpha"); got != content {
		t.Fatalf("alpha content = %q, want %q", got, content)
	}
	if got := mustGet(t, s, "beta"); got != content {
		t.Fatalf("beta content = %q, want %q", got, content)
	}
}

// TestOverwriteKeepsHistory 验证：覆盖写入产生新版本，历史版本仍可读取。
func TestOverwriteKeepsHistory(t *testing.T) {
	s := newTestStore(t)
	inputs := []string{"v1-content", "v2-content-changed", "v3-content-final"}
	t.Logf("input: 对同一对象连续覆盖写入 %q", inputs)

	for _, data := range inputs {
		mustPut(t, s, "doc", data)
	}

	cur, err := s.CurrentVersion(context.Background(), "doc")
	if err != nil {
		t.Fatalf("current version: %v", err)
	}
	t.Logf("判定依据: 当前版本应为 %d，且每个历史版本读回的内容与对应输入一致", len(inputs))
	if cur.ID != int64(len(inputs)) {
		t.Fatalf("current version = %d, want %d", cur.ID, len(inputs))
	}
	if got := mustGet(t, s, "doc"); got != inputs[2] {
		t.Fatalf("current content = %q, want %q", got, inputs[2])
	}
	for i, want := range inputs {
		if got := mustGetVersion(t, s, "doc", int64(i+1)); got != want {
			t.Fatalf("version %d content = %q, want %q", i+1, got, want)
		}
	}
	vs, err := s.Versions(context.Background(), "doc")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(vs) != len(inputs) {
		t.Fatalf("version count = %d, want %d", len(vs), len(inputs))
	}
	// 列表应按版本号从新到旧。
	for i, v := range vs {
		wantID := int64(len(inputs) - i)
		if v.ID != wantID {
			t.Fatalf("versions[%d].ID = %d, want %d", i, v.ID, wantID)
		}
	}
}

// TestLargeAndBinaryPayload 验证二进制与较大数据的往返一致性。
func TestLargeAndBinaryPayload(t *testing.T) {
	s := newTestStore(t)
	data := bytes.Repeat([]byte{0x00, 0xff, 0x42, 0x13}, 1<<18) // 1 MiB 二进制
	t.Logf("input: %d 字节重复二进制模式", len(data))
	v := mustPut(t, s, "blob.bin", string(data))
	rc, _, err := s.Get(context.Background(), "blob.bin")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_ = rc.Close()
	t.Logf("判定依据: 读回 %d 字节与输入逐字节相等，且键 %s 与内容匹配", len(got), v.Key)
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes", len(got))
	}
}

func stringsReader(s string) *strings.Reader { return strings.NewReader(s) }
