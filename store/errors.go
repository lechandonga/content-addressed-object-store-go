package store

import "fmt"

// ErrorKind 描述错误类别，调用方可以据此区分"缺失"与"损坏"等不同原因，
// 而不是只得到一个笼统的 I/O 错误。
type ErrorKind int

const (
	// KindNotFound 表示对象或版本不存在。
	KindNotFound ErrorKind = iota
	// KindCorrupt 表示数据存在但完整性校验失败（内容与摘要不符，或元数据损坏）。
	KindCorrupt
	// KindConflict 表示并发操作产生冲突（例如版本被他人抢先提交）。
	KindConflict
	// KindIO 表示其它底层 I/O 错误。
	KindIO
)

// Error 是本包返回的统一错误类型，携带错误类别与上下文信息。
type Error struct {
	Kind ErrorKind
	msg  string
	err  error
}

func (e *Error) Error() string {
	if e.err != nil {
		return e.msg + ": " + e.err.Error()
	}
	return e.msg
}

// Unwrap 支持 errors.Is/errors.As 解包底层错误。
func (e *Error) Unwrap() error { return e.err }

func newError(kind ErrorKind, msg string, args ...any) *Error {
	return &Error{Kind: kind, msg: fmt.Sprintf(msg, args...)}
}

func wrapError(kind ErrorKind, err error, msg string, args ...any) *Error {
	return &Error{Kind: kind, msg: fmt.Sprintf(msg, args...), err: err}
}

// NotFoundError 标识对象/版本缺失：元数据或数据块在磁盘上根本不存在。
type NotFoundError struct {
	// Obj 为对象名；若为空表示不是对象级别的缺失。
	Obj string
	// Version 为请求的版本号；0 表示请求的是当前版本。
	Version int64
	// Key 为缺失数据块的内容键。为空表示缺失的是对象本身而非其引用的数据块。
	Key ContentKey
}

func (e *NotFoundError) Error() string {
	switch {
	case e.Key != "":
		return fmt.Sprintf("missing blob %s referenced by object %q version %d", e.Key, e.Obj, e.Version)
	case e.Obj != "" && e.Version > 0:
		return fmt.Sprintf("object %q version %d not found", e.Obj, e.Version)
	case e.Obj != "":
		return fmt.Sprintf("object %q not found", e.Obj)
	default:
		return "object not found"
	}
}

// CorruptionError 标识内容寻址数据损坏：文件存在，但重新计算出的摘要
// 与它的地址（内容键）不一致，或元数据无法解析。
type CorruptionError struct {
	// Obj 为关联的对象名（可空）。
	Obj string
	// Version 为关联的版本号（0 表示当前版本或数据块级别）。
	Version int64
	// Key 是数据块声称的内容键。
	Key ContentKey
	// Got 是实际从字节重新计算出的摘要（十六进制），用于解释判定依据。
	Got string
}

func (e *CorruptionError) Error() string {
	if e.Got != "" {
		return fmt.Sprintf("corrupt data for object %q (version %d, key %s): checksum mismatch, recomputed=%s",
			e.Obj, e.Version, e.Key, e.Got)
	}
	return fmt.Sprintf("corrupt data for object %q (version %d, key %s)", e.Obj, e.Version, e.Key)
}
