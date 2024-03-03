package asyncwf

import (
	"AsyncDB/internal/simulation"
	sequentialwf "AsyncDB/internal/simulation/sequential"
	"sync"
)

type AsyncWorkflow struct{}

func (w *AsyncWorkflow) executeSequentialActivities() {
	config := simulation.RandomConfig()
	validationPhase := []func(config *simulation.Config){
		sequentialwf.ValidateCheckout,
		sequentialwf.ValidateAvailability,
		sequentialwf.VerifyCustomer,
		sequentialwf.ValidatePayment,
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func(config *simulation.Config)) {
			defer wg.Done()
			activity(config)
		}(activity)
	}
	wg.Wait()

	operationPhase := []func(config *simulation.Config){
		sequentialwf.RecordOffer,
		sequentialwf.CommitTax,
		sequentialwf.DecrementInventory,
	}
	wg.Add(len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func(config *simulation.Config)) {
			defer wg.Done()
			activity(config)
		}(activity)
	}
	wg.Wait()
	sequentialwf.CompleteOrder(config)
}

func (w *AsyncWorkflow) executeAsyncActivities() {
	config := simulation.RandomConfig()
	validationPhase := []func(config *simulation.Config){
		validateCheckout,
		validateAvailability,
		verifyCustomer,
		validatePayment,
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(validationPhase))
	for _, activity := range validationPhase {
		go func(activity func(config *simulation.Config)) {
			defer wg.Done()
			activity(config)
		}(activity)
	}
	wg.Wait()

	operationPhase := []func(config *simulation.Config){
		recordOffer,
		commitTax,
		decrementInventory,
	}
	wg.Add(len(operationPhase))
	for _, activity := range operationPhase {
		go func(activity func(config *simulation.Config)) {
			defer wg.Done()
			activity(config)
		}(activity)
	}
	wg.Wait()
	completeOrder(config)
}

func (w *AsyncWorkflow) ExecuteAsync() {
	w.executeAsyncActivities()
}

func (w *AsyncWorkflow) ExecuteSequential() {
	w.executeSequentialActivities()
}
