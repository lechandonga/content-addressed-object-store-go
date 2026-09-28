package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

// 寻址算法相关常量。当前固定使用 SHA-256，地址为摘要的小写十六进制。
const (
	hashAlgo   = "sha256"
	hashHexLen = 64
)

// contentAddress 返回数据的内容地址（SHA-256 十六进制摘要）。
// 相同内容必然得到相同地址，这是去重与完整性校验的共同依据。
func contentAddress(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hashReader 读取 r 的全部内容，同时计算其 SHA-256 摘要与字节数。
func hashReader(r io.Reader) (addr string, n int64, err error) {
	h := sha256.New()
	n, err = io.Copy(h, r)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
