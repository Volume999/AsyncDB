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

func (w *SequentialWorkflow) Execute() {
	w.s.ValidateCheckout()
	w.s.ValidateAvailability()
	w.s.VerifyCustomer()
	w.s.ValidatePayment()
	w.s.RecordOffer()
	w.s.CommitTax()
	w.s.DecrementInventory()
	w.s.CompleteOrder()
}
