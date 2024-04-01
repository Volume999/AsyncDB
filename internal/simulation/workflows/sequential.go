package workflows

import (
	"AsyncDB/internal/simulation/activities"
)

type SequentialWorkflow struct {
	s activities.Simulator
}

func NewSequentialWorkflow(simulator activities.Simulator) *SequentialWorkflow {
	return &SequentialWorkflow{s: simulator}
}

func execFnAsync(f func()) {
	c := make(chan struct{})
	go func() {
		f()
		close(c)
	}()
	<-c
}

func (w *SequentialWorkflow) Execute() {
	//w.s.ValidateCheckout()
	//w.s.ValidateAvailability()
	//w.s.VerifyCustomer()
	//w.s.ValidatePayment()
	//w.s.RecordOffer()
	//w.s.CommitTax()
	//w.s.DecrementInventory()
	//w.s.CompleteOrder()
	execFnAsync(w.s.ValidateCheckout)
	execFnAsync(w.s.ValidateAvailability)
	execFnAsync(w.s.VerifyCustomer)
	execFnAsync(w.s.ValidatePayment)
	execFnAsync(w.s.ValidateProductOption)
	execFnAsync(w.s.RecordOffer)
	execFnAsync(w.s.CommitTax)
	execFnAsync(w.s.DecrementInventory)
	execFnAsync(w.s.CompleteOrder)
}
