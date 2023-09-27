package services

import (
	"fmt"
)

type Command struct {
	Data   interface{}
	Result chan<- Response
}

type Response struct {
	Data  interface{}
	Error error
}

var ErrUnknownCommand = fmt.Errorf("unknown command")
