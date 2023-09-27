package local

import (
	"POCS_Projects/internal/models"
	"POCS_Projects/internal/services"
	"POCS_Projects/internal/stores"
	"log"
)

type localAccountService struct {
	store          stores.AccountStore
	logger         *log.Logger
	commandChannel <-chan services.Command
}

const (
	GetAccountsCommand    = "get_accounts"
	GetAccountByIDCommand = "get_account_by_id"
	CreateAccountCommand  = "create_account"
	UpdateAccountCommand  = "update_account"
	DeleteAccountCommand  = "delete_account"
)

func (a *localAccountService) GetAccounts() ([]models.Customer, error) {
	return a.store.GetAccounts()
}

func (a *localAccountService) GetAccountByID(id int) (*models.Customer, error) {
	return a.store.GetAccountByID(id)
}

func (a *localAccountService) CreateAccount(acc *models.Customer) error {
	return a.store.CreateAccount(acc)
}

func (a *localAccountService) UpdateAccount(acc *models.Customer) error {
	return a.store.UpdateAccount(acc)
}

func (a *localAccountService) DeleteAccount(id int) error {
	return a.store.DeleteAccount(id)
}

//func (a *localAccountService) handleCommand(cmd services.Command) {
//	switch cmd.Action {
//	case GetAccountsCommand:
//		a.logger.Println("get_accounts")
//		accounts, err := a.GetAccounts()
//		cmd.Result <- services.Response{Data: accounts, Error: err}
//	case GetAccountByIDCommand:
//		a.logger.Println("get_account_by_id")
//		acc, err := a.GetAccountByID(cmd.AccountId)
//		cmd.Result <- services.Response{Data: acc, Error: err}
//	case CreateAccountCommand:
//		a.logger.Println("create_account")
//		err := a.CreateAccount(cmd.Account)
//		cmd.Result <- services.Response{Error: err}
//	case UpdateAccountCommand:
//		a.logger.Println("update_account")
//		err := a.UpdateAccount(cmd.Account)
//		cmd.Result <- services.Response{Error: err}
//	case DeleteAccountCommand:
//		a.logger.Println("delete_account")
//		err := a.DeleteAccount(cmd.Account.ID)
//		cmd.Result <- services.Response{Error: err}
//	default:
//		a.logger.Println("unknown command")
//		cmd.Result <- services.Response{Error: services.ErrUnknownCommand}
//	}
//}

//func (a *localAccountService) Run() {
//	a.logger.Println("starting account service")
//	for cmd := range a.commandChannel {
//		a.handleCommand(cmd)
//	}
//}

func NewLocalAccountService(store stores.AccountStore, logger *log.Logger, commandChannel <-chan services.Command) services.CustomerService {
	return &localAccountService{store, logger, commandChannel}
}
