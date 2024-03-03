package sequentialwf

import "AsyncDB/internal/simulation"

type SequentialWorkflow struct {
}

func (w *SequentialWorkflow) ExecuteSequential() {
	config := simulation.RandomConfig()
	ValidateCheckout(config)
	ValidateAvailability(config)
	VerifyCustomer(config)
	ValidatePayment(config)
	RecordOffer(config)
	CommitTax(config)
	DecrementInventory(config)
	CompleteOrder(config)
}

func (w *SequentialWorkflow) ExecuteAsync() {
	panic("implement me")
}
