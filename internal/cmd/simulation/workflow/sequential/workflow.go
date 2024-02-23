package sequentialwf

type SequentialWorkflow struct {
}

func (w *SequentialWorkflow) ExecuteSequential() {
	ValidateCheckout()
	ValidateAvailability()
	VerifyCustomer()
	ValidatePayment()
	RecordOffer()
	CommitTax()
	DecrementInventory()
	CompleteOrder()
}

func (w *SequentialWorkflow) ExecuteAsync() {
	panic("implement me")
}
