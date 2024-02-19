package main

import (
	async "AsyncDB/internal/cmd/simulation/workflow/async"
	sequential "AsyncDB/internal/cmd/simulation/workflow/sequential"
	"testing"
)

func BenchmarkSequentialWorkflow(b *testing.B) {
	w := &sequential.SequentialWorkflow{}
	b.SetParallelism(100000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Execute()
		}
	})
}

func BenchmarkAsyncWorkflowSequentialActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	b.SetParallelism(100000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Execute(false)
		}
	})
}

func BenchmarkAsyncWorkflowAsyncActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	b.SetParallelism(100000)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w.Execute(true)
		}
	})
}
