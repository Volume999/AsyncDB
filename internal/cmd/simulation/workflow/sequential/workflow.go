package sequentialwf

type SequentialWorkflow struct {
}

func (w *SequentialWorkflow) Execute() {
	ValidateCheckout()
	ValidateAvailability()
	VerifyCustomer()
	ValidatePayment()
	RecordOffer()
	CommitTax()
	DecrementInventory()
	CompleteOrder()
}
