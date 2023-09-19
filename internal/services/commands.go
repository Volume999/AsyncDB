package services

import "POCS_Projects/internal/data"

type Command struct {
	Action  string
	Account *data.Account
	Result  chan<- Response
}

type Response struct {
	Account *data.Account
	Error   error
}
