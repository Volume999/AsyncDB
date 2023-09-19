package services

import (
	"POCS_Projects/internal/data"
	"log"
)

type AccountService interface {
	GetAccounts() ([]data.Account, error)
	GetAccountByID(id int) (*data.Account, error)
	CreateAccount(a *data.Account) error
	UpdateAccount(a *data.Account) error
	DeleteAccount(id int) error
	Run()
}

type localAccountService struct {
	store          data.AccountStore
	logger         *log.Logger
	commandChannel <-chan Command
}

func (a *localAccountService) GetAccounts() ([]data.Account, error) {
	//TODO implement me
	panic("implement me")
}

func (a *localAccountService) GetAccountByID(id int) (*data.Account, error) {
	//TODO implement me
	panic("implement me")
}

func (a *localAccountService) CreateAccount(acc *data.Account) error {
	//TODO implement me
	panic("implement me")
}

func (a *localAccountService) UpdateAccount(acc *data.Account) error {
	//TODO implement me
	panic("implement me")
}

func (a *localAccountService) DeleteAccount(id int) error {
	//TODO implement me
	panic("implement me")
}

func (a *localAccountService) Run() {
	a.logger.Println("starting account service")
	for cmd := range a.commandChannel {
		switch cmd.Action {
		case "get_accounts":
			a.logger.Println("get_accounts")
			//cmd.Result <- Response{
			//	Account: nil,
			//	Error:   nil,
			//}
			break
		case "get_account_by_id":
			a.logger.Println("get_account_by_id")
			break
		case "create_account":
			a.logger.Println("create_account")
			break
		case "update_account":
			a.logger.Println("update_account")
			break
		case "delete_account":
			a.logger.Println("delete_account")
			break
		default:
			a.logger.Println("unknown command")
		}
	}
}

func NewLocalAccountService(store data.AccountStore, logger *log.Logger, commandChannel <-chan Command) AccountService {
	return &localAccountService{store, logger, commandChannel}
}
