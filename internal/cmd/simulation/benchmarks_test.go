package main

import (
	async "AsyncDB/internal/cmd/simulation/workflow/async"
	sequential "AsyncDB/internal/cmd/simulation/workflow/sequential"
	"testing"
)

func BenchmarkSequentialWorkflow(b *testing.B) {
	w := &sequential.SequentialWorkflow{}
	for i := 0; i < b.N; i++ {
		w.Execute()
	}
}

func BenchmarkAsyncWorkflowSequentialActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	for i := 0; i < b.N; i++ {
		w.Execute(false)
	}
}

func BenchmarkAsyncWorkflowAsyncActivities(b *testing.B) {
	w := &async.AsyncWorkflow{}
	for i := 0; i < b.N; i++ {
		w.Execute(true)
	}
}
