package main

import (
	"POCS_Projects/internal/config"
	"POCS_Projects/internal/services/order"
	"POCS_Projects/internal/services/order/cmd"
	"fmt"
	"log"
	"os"
)

const (
	configPath = "./internal/config/"
)

func main() {
	l := log.New(os.Stdout, "NewOrderCLI: ", log.LstdFlags)
	appConfig, err := config.LoadConfig(configPath)
	if err != nil {
		l.Fatal(err)
	}

	var orderService order.Service

	if appConfig.OrderServiceImplementation == "monoservice" {
		orderService = order.NewMonoService(l)
	}

	orderService.CreateOrder(cmd.NewOrderCommand{})

	fmt.Println("Hello, World!")
}
