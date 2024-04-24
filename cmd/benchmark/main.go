package main

import (
	"AsyncDB/internal/tpcc/config"
	"AsyncDB/internal/tpcc/dataloaders"
	"fmt"
	"time"
)

func benchmarkDataGeneration() {
	// Try generating data
	constants := config.NewConstants()
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
