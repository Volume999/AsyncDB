package main

import (
	async "AsyncDB/internal/cmd/simulation/workflow/async"
	sequentialwf "AsyncDB/internal/cmd/simulation/workflow/sequential"
	"strconv"
	"testing"
)

type Workflow interface {
	ExecuteSequential()
	ExecuteAsync()
}

func benchmarkWorkflow(w Workflow, b *testing.B, async bool) {
	parallelisms := []int{1, 10, 100, 1000, 10000, 100000}
	f := func() {
		if async {
			w.ExecuteAsync()
		} else {
			w.ExecuteSequential()
		}
	}
	b.ResetTimer()
	for _, parallelism := range parallelisms {
		b.SetParallelism(parallelism)
		b.Run("parallelism="+strconv.Itoa(parallelism), func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					f()
				}
			})
		})
	}
}

func BenchmarkSequentialWorkflow(b *testing.B) {
	w := &sequentialwf.SequentialWorkflow{}
	benchmarkWorkflow(w, b, false)
}

func BenchmarkAsyncWorkflowSequentialActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	benchmarkWorkflow(w, b, false)
}

func BenchmarkAsyncWorkflowAsyncActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	benchmarkWorkflow(w, b, true)
}
