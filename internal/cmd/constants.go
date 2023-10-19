package commands

import (
	"POCS_Projects/internal/cmd/benchmark/dataloaders/generators"
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
