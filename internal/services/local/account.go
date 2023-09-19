package local

import (
	"POCS_Projects/internal/data"
	"POCS_Projects/internal/services"
	"log"
)

type localAccountService struct {
	store          data.AccountStore
	logger         *log.Logger
	commandChannel <-chan services.Command
}

func (a *localAccountService) GetAccounts() ([]data.Account, error) {
	//TODO implement me
	return a.store.GetAccounts()
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
			if accounts, err := a.GetAccounts(); err != nil {
				cmd.Result <- services.Response{Data: nil, Error: err}
			} else {
				cmd.Result <- services.Response{Data: accounts, Error: nil}
			}
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

func NewLocalAccountService(store data.AccountStore, logger *log.Logger, commandChannel <-chan services.Command) services.AccountService {
	return &localAccountService{store, logger, commandChannel}
}
