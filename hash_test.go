package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestContentAddress(t *testing.T) {
	data := []byte("addressing-vectors")
	got := contentAddress(data)
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	t.Logf("输入: %q ⇒ 地址 %s", string(data), got)
	if got != want {
		t.Fatalf("判定依据: 地址应为标准 SHA-256 十六进制 %s，实际 %s", want, got)
	}
	if len(got) != hashHexLen {
		t.Fatalf("判定依据: 地址长度应为 %d，实际 %d", hashHexLen, len(got))
	}
	if contentAddress(data) != contentAddress(append([]byte(nil), data...)) {
		t.Fatal("判定依据: 相同内容必须得到相同地址")
	}
	if contentAddress([]byte("other")) == got {
		t.Fatal("判定依据: 不同内容不应得到相同地址")
	}
	t.Log("判定依据: 相同内容同址、不同内容异址，长度=64，符合 SHA-256 十六进制")
}

func TestHashReader(t *testing.T) {
	data := bytes.Repeat([]byte("stream"), 1000)
	addr, n, err := hashReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: 流式读取 %d 字节 ⇒ 地址 %s", n, addr)
	if n != int64(len(data)) || addr != contentAddress(data) {
		t.Fatalf("判定依据: 字节数与摘要应与一次性计算一致 n=%d addr=%s", n, addr)
	}
}
