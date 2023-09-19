package main

import (
	"POCS_Projects/internal/data/memory"
	"POCS_Projects/internal/services"
	"fmt"
	"log"
	"os"
)

func main() {
	logger := log.New(os.Stdout, "account-service", log.LstdFlags)
	accountStore := memory.NewLocalAccountStore()
	accountServiceChannel := make(chan services.Command)
	accountService := services.NewLocalAccountService(accountStore, logger, accountServiceChannel)
	go accountService.Run()
	responseChan := make(chan services.Response)
	accountServiceChannel <- services.Command{
		Action: "get_accounts",
		Result: responseChan,
	}
	response := <-responseChan
	fmt.Println("Response:", response)
}
