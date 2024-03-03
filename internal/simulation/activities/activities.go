package activities

type Simulator interface {
	ValidateCheckout()
	ValidateAvailability()
	VerifyCustomer()
	ValidatePayment()
	ValidateProductOption()
	RecordOffer()
	CommitTax()
	DecrementInventory()
	CompleteOrder()
}
