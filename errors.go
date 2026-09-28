// Package cas 实现了一个基于本地磁盘的内容寻址对象存储。
//
// 数据以其内容的 SHA-256 摘要寻址，天然去重；每个逻辑键维护一条
// 只追加的版本链；GC 只回收不被任何版本引用的数据块。
package cas

import "errors"

// 对外暴露的哨兵错误，调用方可用 errors.Is 区分不同失败原因：
//   - ErrNotFound / ErrVersionNotFound：元数据中不存在该键或版本；
//   - ErrDataMissing：元数据引用了某个数据块，但磁盘文件缺失；
//   - ErrCorrupt：数据块存在，但重新计算的摘要或大小与寻址地址不符。
var (
	ErrNotFound        = errors.New("cas: key not found")
	ErrVersionNotFound = errors.New("cas: version not found")
	ErrDataMissing     = errors.New("cas: referenced object data is missing on disk")
	ErrCorrupt         = errors.New("cas: object data is corrupted (checksum mismatch)")
	ErrInvalidArgument = errors.New("cas: invalid argument")
)
