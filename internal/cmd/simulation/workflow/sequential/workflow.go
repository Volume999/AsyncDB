package workflow

type SequentialWorkflow struct {
}

func (w *SequentialWorkflow) Execute() {
	validateCheckout()
	validateAvailability()
	verifyCustomer()
	validatePayment()
	recordOffer()
	commitTax()
	decrementInventory()
	completeOrder()
}
