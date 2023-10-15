package generators

import (
	"math"
	"math/rand"
	"strconv"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func NURand(A int, x int, y int, C int) int {
	return (((RandomIntInRange(0, A) | RandomIntInRange(x, y)) + C) % (y - x + 1)) + x
}

func RandomNStrRangeLen(i int, i2 int) string {
	beg := 10 * i
	end := 10*(i2+1) - 1
	val := RandomIntInRange(beg, end)
	return strconv.Itoa(val)
}

func RandomFloatInRange(f float64, f2 float64, prec int) float64 {
	multiplier := math.Pow10(prec)
	return float64(RandomIntInRange(int(f*multiplier), int(f2*multiplier))) / multiplier
}

func RandomStrRangeLen(i int, i2 int) string {
	strlen := RandomIntInRange(i, i2)
	str := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		str[i] = charset[RandomIntInRange(0, len(charset)-1)]
	}
	return string(str)
}

func RandomIntInRange(i int, i2 int) int {
	return rand.Intn(i2-i+1) + i
}

func RandomPermutationInt(i int, i2 int) []int {
	perm := make([]int, i2-i+1)
	for c := i; c <= i2; c++ {
		perm[c-i] = c
	}
	rand.Shuffle(len(perm), func(i, j int) { perm[i], perm[j] = perm[j], perm[i] })
	return perm
}
