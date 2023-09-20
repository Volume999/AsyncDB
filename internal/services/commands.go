package services

import (
	"POCS_Projects/internal/data"
	"fmt"
)

type Command struct {
	Action  string
	Account *data.Account
	Result  chan<- Response
}

type Response struct {
	Data  interface{}
	Error error
}

var ErrUnknownCommand = fmt.Errorf("unknown command")
