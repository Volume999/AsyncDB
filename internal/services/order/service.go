package order

type Service interface {
	// CreateOrder creates a new order
	CreateOrder(command Command) Response
}
