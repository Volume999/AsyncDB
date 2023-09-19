package main

import (
	dataLocal "POCS_Projects/internal/data/local"
	"POCS_Projects/internal/services"
	servicesLocal "POCS_Projects/internal/services/local"
	"fmt"
	"log"
	"os"
)

func main() {
	logger := log.New(os.Stdout, "account-service", log.LstdFlags)
	accountStore := dataLocal.NewLocalAccountStore()
	accountServiceChannel := make(chan services.Command)
	accountService := servicesLocal.NewLocalAccountService(accountStore, logger, accountServiceChannel)
	go accountService.Run()
	responseChan := make(chan services.Response)
	accountServiceChannel <- services.Command{
		Action: "get_accounts",
		Result: responseChan,
	}
	response := <-responseChan
	fmt.Println("Response:", response)
}
