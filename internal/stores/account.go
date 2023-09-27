package stores

import (
	"POCS_Projects/internal/models"
	"fmt"
)

type AccountStore interface {
	GetAccounts() ([]models.Customer, error)
	GetAccountByID(id int) (*models.Customer, error)
	CreateAccount(a *models.Customer) error
	UpdateAccount(a *models.Customer) error
	DeleteAccount(id int) error
}

var ErrAccountNotFound = fmt.Errorf("account not found")
