package benchmark

import (
	"POCS_Projects/internal/benchmark/dataloaders/generators"
)

type Constants struct {
	CLast int
	CID   int
	COLID int
}

func NewConstants() Constants {
	return Constants{
		CLast: generators.RandomIntInRange(0, 255),
		CID:   generators.RandomIntInRange(0, 1023),
		COLID: generators.RandomIntInRange(0, 8191),
	}
}
