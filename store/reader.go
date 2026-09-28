package store

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// newSha256 返回一个新的 SHA-256 摘要器。
func newSha256() hash.Hash { return sha256.New() }

// verifiedReader 包装底层数据块文件：读取过程中增量计算摘要，
// 在数据全部读完且 Close 时比对内容键，任何不一致都报 CorruptionError。
// 这样"缺失"（文件打不开）与"损坏"（读到了但摘要对不上）被明确区分。
type verifiedReader struct {
	rc       io.ReadCloser
	h        hash.Hash
	want     ContentKey
	wantSize int64
	gotSize  int64
	obj      string
	version  int64
	release  func()
	closed   bool
}

func (v *verifiedReader) Read(p []byte) (int, error) {
	n, err := v.rc.Read(p)
	if n > 0 {
		_, _ = v.h.Write(p[:n])
		v.gotSize += int64(n)
	}
	if err == io.EOF {
		if cerr := v.verify(); cerr != nil {
			return n, cerr
		}
	}
	return n, err
}

func (v *verifiedReader) verify() error {
	got := ContentKey(hex.EncodeToString(v.h.Sum(nil)))
	if got != v.want || v.gotSize != v.wantSize {
		return &CorruptionError{
			Obj:     v.obj,
			Version: v.version,
			Key:     v.want,
			Got:     string(got),
		}
	}
	return nil
}

// Close 对未读完（例如提前关闭）的读取也尽力校验：若长度不足或摘要不符，
// 返回 CorruptionError，避免截断的数据被当成正常结果消费。
func (v *verifiedReader) Close() error {
	if v.closed {
		return nil
	}
	v.closed = true
	var verifyErr error
	if v.gotSize != v.wantSize {
		// 未完整读取（被截断或调用方提前关闭），无法得到完整摘要，
		// 仍按损坏报告，并附上期望/实际长度。
		verifyErr = &CorruptionError{
			Obj:     v.obj,
			Version: v.version,
			Key:     v.want,
			Got:     "short-read: size " + itoa(v.gotSize) + ", want " + itoa(v.wantSize),
		}
	} else if err := v.verify(); err != nil {
		verifyErr = err
	}
	cerr := v.rc.Close()
	v.release()
	if verifyErr != nil {
		return verifyErr
	}
	return cerr
}
