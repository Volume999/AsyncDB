package local

import (
	"POCS_Projects/internal/data"
)

type localAccountStore struct {
	accounts []data.Account
}

func (l localAccountStore) GetAccounts() ([]data.Account, error) {
	return l.accounts, nil
}

func (l localAccountStore) GetAccountByID(id int) (*data.Account, error) {
	for i := range l.accounts {
		acc := l.accounts[i]
		if acc.ID == id {
			return &acc, nil
		}
	}
	return nil, data.ErrAccountNotFound
}

func getNextID(accounts []data.Account) int {
	maxID := 0
	for _, acc := range accounts {
		if acc.ID > maxID {
			maxID = acc.ID
		}
	}
	return maxID + 1
}

func (l localAccountStore) CreateAccount(a *data.Account) error {
	a.ID = getNextID(l.accounts)
	l.accounts = append(l.accounts, *a)
	return nil
}

func (l localAccountStore) UpdateAccount(a *data.Account) error {
	if acc, err := l.GetAccountByID(a.ID); err != nil {
		return err
	} else {
		acc.Name = a.Name
		acc.Balance = a.Balance
		return nil
	}
}

func (l localAccountStore) DeleteAccount(id int) error {
	for i := range l.accounts {
		if l.accounts[i].ID == id {
			l.accounts = append(l.accounts[:i], l.accounts[i+1:]...)
			return nil
		}
	}
	return data.ErrAccountNotFound
}

var localAccounts = []data.Account{
	{
		ID:      1,
		Name:    "John Doe",
		Balance: 1000.50,
	},
	{
		ID:      2,
		Name:    "Jane Smith",
		Balance: 1500.75,
	},
	// ... add more accounts as needed
}

func NewLocalAccountStore() data.AccountStore {
	return &localAccountStore{
		accounts: localAccounts,
	}
}
