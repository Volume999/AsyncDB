package services

import (
	"POCS_Projects/internal/models"
)

type CustomerService interface {
	GetAccounts() ([]models.Customer, error)
	GetAccountByID(id int) (*models.Customer, error)
	CreateAccount(a *models.Customer) error
	UpdateAccount(a *models.Customer) error
	DeleteAccount(id int) error
	//Run()
}
