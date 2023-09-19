package data

import "fmt"

type Account struct {
	ID      int
	Name    string
	Balance float64
}

type AccountStore interface {
	GetAccounts() ([]Account, error)
	GetAccountByID(id int) (*Account, error)
	CreateAccount(a *Account) error
	UpdateAccount(a *Account) error
	DeleteAccount(id int) error
}

var ErrAccountNotFound = fmt.Errorf("account not found")
