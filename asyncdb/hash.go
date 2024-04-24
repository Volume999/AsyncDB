package asyncdb

import "hash/fnv"

type StringHasher struct{}

func NewStringHasher() *StringHasher {
	return &StringHasher{}
}

func (sh *StringHasher) HashStringUint64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
