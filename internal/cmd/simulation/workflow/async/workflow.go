package asyncwf

import (
	sequentialwf "AsyncDB/internal/cmd/simulation/workflow/sequential"
	"sync"
)

type AsyncWorkflow struct{}

func (w *AsyncWorkflow) executeSequentialActivities() {
	validationPhase := []func(){
		sequentialwf.ValidateCheckout,
		sequentialwf.ValidateAvailability,
		sequentialwf.VerifyCustomer,
		sequentialwf.ValidatePayment,
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()

	operationPhase := []func(){
		sequentialwf.RecordOffer,
		sequentialwf.CommitTax,
		sequentialwf.DecrementInventory,
	}
	wg.Add(len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()
	sequentialwf.CompleteOrder()
}

func (w *AsyncWorkflow) executeAsyncActivities() {
	validationPhase := []func(){
		validateCheckout,
		validateAvailability,
		verifyCustomer,
		validatePayment,
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()

	operationPhase := []func(){
		recordOffer,
		commitTax,
		decrementInventory,
	}
	wg.Add(len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func()) {
			defer wg.Done()
			activity()
		}(activity)
	}
	wg.Wait()
	completeOrder()
}

func (w *AsyncWorkflow) ExecuteAsync() {
	w.executeAsyncActivities()
}

func (w *AsyncWorkflow) ExecuteSequential() {
	w.executeSequentialActivities()
}
