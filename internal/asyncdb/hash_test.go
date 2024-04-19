package asyncdb

import "testing"

func FuzzHashStringUint64(f *testing.F) {
	h := NewStringHasher()
	f.Add("table", "FuzzTable")
	f.Fuzz(func(t *testing.T, s1 string, s2 string) {
		if h.HashStringUint64(s1) == h.HashStringUint64(s2) {
			if s1 != s2 {
				t.Errorf("hash collision: %s == %s", s1, s2)
			}
		}
		if h.HashStringUint64(s1) != h.HashStringUint64(s1) || h.HashStringUint64(s2) != h.HashStringUint64(s2) {
			t.Errorf("hash not consistent: %s, %s", s1, s2)
		}
	})
}
