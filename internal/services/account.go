package services

import (
	"POCS_Projects/internal/data"
)

type AccountService interface {
	GetAccounts() ([]data.Account, error)
	GetAccountByID(id int) (*data.Account, error)
	CreateAccount(a *data.Account) error
	UpdateAccount(a *data.Account) error
	DeleteAccount(id int) error
	Run()
}
