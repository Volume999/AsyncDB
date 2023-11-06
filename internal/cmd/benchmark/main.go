package main

import (
	"POCS_Projects/internal/cmd/benchmark/dataloaders"
	commands2 "POCS_Projects/internal/config"
	"fmt"
	"time"
)

func benchmarkDataGeneration() {
	// Try generating data
	constants := commands2.NewConstants()
	times := 0
	for i := 0; i < 10; i++ {
		start := time.Now()
		_, _ = dataloaders.NewDataGeneratorImpl(10, constants, nil).GenerateData()
		t := time.Since(start).Seconds()
		fmt.Println(t)
		times += int(t)
	}
	fmt.Println("Average time taken: ", times/10.0)
}

func main() {
	fmt.Println("Hello, benchmark")
}
