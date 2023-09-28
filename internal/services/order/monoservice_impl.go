package order

import (
	"POCS_Projects/internal/services/order/cmd"
	"log"
)

type MonoService struct {
	l *log.Logger
}

func NewMonoService(l *log.Logger) *MonoService {
	return &MonoService{l: l}
}

func (s *MonoService) CreateOrder(command cmd.NewOrderCommand) cmd.NewOrderResponse {
	s.l.Println("CreateOrder")
	response := cmd.NewOrderResponse{
		ExecutionStatus: "Not Implemented",
	}
	return response
}
