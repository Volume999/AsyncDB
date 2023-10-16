package generators

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestNURand(t *testing.T) {
	type args struct {
		A int
		x int
		y int
		C int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NURand(tt.args.A, tt.args.x, tt.args.y, tt.args.C); got != tt.want {
				t.Errorf("NURand() = %v, want %v", got, tt.want)
			}
		})
	}
}

func decimalPortion(n float64) int {
	decimalPlaces := fmt.Sprintf("%f", n-math.Floor(n))          // produces 0.xxxx0000
	decimalPlaces = strings.Replace(decimalPlaces, "0.", "", -1) // remove 0.
	decimalPlaces = strings.TrimRight(decimalPlaces, "0")        // remove trailing 0s
	return len(decimalPlaces)
}

func TestRandomFloatInRange(t *testing.T) {
	type args struct {
		f1   float64
		f2   float64
		prec int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Result should have the same precision as the precision argument",
			args: args{
				f1:   1.5,
				f2:   2.5,
				prec: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxPrec := 0
			for i := 0; i < 100; i++ {
				got := RandomFloatInRange(tt.args.f1, tt.args.f2, tt.args.prec)
				gotPrecision := decimalPortion(got)
				fmt.Println(got, gotPrecision)
				if gotPrecision > tt.args.prec {
					t.Errorf("Result %v has more precision than the precision argument %v", got, tt.args.prec)
				}
				if maxPrec < gotPrecision {
					maxPrec = gotPrecision
				}
			}
			if maxPrec < tt.args.prec {
				t.Errorf("Maximum Precision: %v has less precision than the precision argument %v", maxPrec, tt.args.prec)
			}
		})
	}
}

func TestRandomIntInRange(t *testing.T) {
	type args struct {
		i  int
		i2 int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RandomIntInRange(tt.args.i, tt.args.i2); got != tt.want {
				t.Errorf("RandomIntInRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandomNStrRangeLen(t *testing.T) {
	type args struct {
		i  int
		i2 int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RandomNStrRangeLen(tt.args.i, tt.args.i2); got != tt.want {
				t.Errorf("RandomNStrRangeLen() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandomPermutationInt(t *testing.T) {
	type args struct {
		i  int
		i2 int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RandomPermutationInt(tt.args.i, tt.args.i2); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RandomPermutationInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRandomStrRangeLen(t *testing.T) {
	type args struct {
		i  int
		i2 int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RandomStrRangeLen(tt.args.i, tt.args.i2); got != tt.want {
				t.Errorf("RandomStrRangeLen() = %v, want %v", got, tt.want)
			}
		})
	}
}
