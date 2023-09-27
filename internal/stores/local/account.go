package local

import (
	"POCS_Projects/internal/models"
	"POCS_Projects/internal/stores"
)

type localAccountStore struct {
	accounts []models.Customer
}

func (l localAccountStore) GetAccounts() ([]models.Customer, error) {
	return l.accounts, nil
}

func (l localAccountStore) GetAccountByID(id int) (*models.Customer, error) {
	for i := range l.accounts {
		acc := l.accounts[i]
		if acc.ID == id {
			return &acc, nil
		}
	}
	return nil, stores.ErrAccountNotFound
}

func getNextID(accounts []models.Customer) int {
	maxID := 0
	for _, acc := range accounts {
		if acc.ID > maxID {
			maxID = acc.ID
		}
	}
	return maxID + 1
}

func (l localAccountStore) CreateAccount(a *models.Customer) error {
	a.ID = getNextID(l.accounts)
	l.accounts = append(l.accounts, *a)
	return nil
}

func (l localAccountStore) UpdateAccount(a *models.Customer) error {
	return nil
}

func (l localAccountStore) DeleteAccount(id int) error {
	for i := range l.accounts {
		if l.accounts[i].ID == id {
			l.accounts = append(l.accounts[:i], l.accounts[i+1:]...)
			return nil
		}
	}
	return stores.ErrAccountNotFound
}

func NewLocalAccountStore() stores.AccountStore {
	return &localAccountStore{}
}
