package store

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
)

// streamingHasher 在数据落盘的同时增量计算内容键。
type streamingHasher struct{ h hash.Hash }

func newHasher() *streamingHasher { return &streamingHasher{h: sha256.New()} }

func (x *streamingHasher) Write(p []byte) (int, error) { return x.h.Write(p) }

func (x *streamingHasher) key() ContentKey {
	return ContentKey(hex.EncodeToString(x.h.Sum(nil)))
}
