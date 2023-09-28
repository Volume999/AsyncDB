package order

import "POCS_Projects/internal/services/order/cmd"

type Service interface {
	// CreateOrder creates a new order
	CreateOrder(command cmd.NewOrderCommand) cmd.NewOrderResponse
}
