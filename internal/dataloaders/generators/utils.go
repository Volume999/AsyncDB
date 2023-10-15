package generators

func NURand(A int, x int, y int, C int) int {
	return (((RandomIntInRange(0, A) | RandomIntInRange(x, y)) + C) % (y - x + 1)) + x
}

func RandomNStrRangeLen(_i int, _i2 int) string {
	panic("implement me")
}

func RandomFloatInRange(f float64, f2 float64, pres int) float64 {
	return 0
}

func RandomStrRangeLen(i int, i2 int) string {
	return "0"
}

func RandomIntInRange(i int, i2 int) int {
	return 0
}

func RandomPermutationInt(i int, i2 int) []int {
	panic("implement me")
}
