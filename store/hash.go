package store

import "path"

// ContentKey 是数据内容的 SHA-256 摘要（十六进制字符串），是数据块的唯一地址。
// 相同内容必然得到相同的键，因此天然支持去重合并。
type ContentKey string

const (
	// shardBits 每层分片取摘要前缀的字符数。
	shardBits = 2
	// 完整十六进制 SHA-256 摘要长度。
	sha256HexLen = 64
)

// String 返回内容键的十六进制文本。
func (k ContentKey) String() string { return string(k) }

// valid 判断内容键格式是否合法（64 位十六进制字符）。
func (k ContentKey) valid() bool {
	if len(k) != sha256HexLen {
		return false
	}
	for _, c := range k {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}

// shardPath 返回内容键相对于 blobs 目录的两层分片路径，如
// "ab/cd/abcdef...."。分片是为了避免单目录文件过多。
func (k ContentKey) shardPath() string {
	name := string(k)
	return path.Join(name[:shardBits], name[shardBits:shardBits+shardBits], name)
}
