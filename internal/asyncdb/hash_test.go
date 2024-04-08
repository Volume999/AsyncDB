package asyncdb

import "testing"

func FuzzHashStringUint64(f *testing.F) {
	f.Add("table", "FuzzTable")
	f.Fuzz(func(t *testing.T, s1 string, s2 string) {
		if HashStringUint64(s1) == HashStringUint64(s2) {
			if s1 != s2 {
				t.Errorf("hash collision: %s == %s", s1, s2)
			}
		}
		if HashStringUint64(s1) != HashStringUint64(s1) || HashStringUint64(s2) != HashStringUint64(s2) {
			t.Errorf("hash not consistent: %s, %s", s1, s2)
		}
	})
}
