package simulation

import (
	"AsyncDB/internal/simulation/activities"
	"AsyncDB/internal/simulation/workflows"
	"strconv"
	"testing"
)

func benchmarkWorkflow(w workflows.Workflow, b *testing.B, async bool) {
	parallelisms := []int{1, 10, 100, 1000, 10000, 100000}
	f := func() {
		w.Execute()
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
	config := activities.RandomConfig()
	simulator := activities.NewSequentialSimulator(config)
	w := workflows.NewSequentialWorkflow(simulator)
	benchmarkWorkflow(w, b, false)
}

func BenchmarkAsyncWorkflowSequentialActivities(b *testing.B) {
	config := activities.RandomConfig()
	simulator := activities.NewSequentialSimulator(config)
	w := workflows.NewAsyncWorkflow(simulator)
	benchmarkWorkflow(w, b, false)
}

func BenchmarkAsyncWorkflowAsyncActivities(b *testing.B) {
	config := activities.RandomConfig()
	simulator := activities.NewAsyncSimulator(config)
	w := workflows.NewAsyncWorkflow(simulator)
	benchmarkWorkflow(w, b, true)
}
