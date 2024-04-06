package asyncdb

import "hash/fnv"

func HashStringUint64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
