package async

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/databases"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
	"log"
)

type CustomerStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func (c *CustomerStore) Put(ctx *asyncdb.ConnectionContext, value models.Customer) <-chan databases.RequestResult {
	return c.db.Put(ctx, "Customer", models.CustomerPK{ID: value.ID}, value)
}

func (c *CustomerStore) Get(ctx *asyncdb.ConnectionContext, key models.CustomerPK) <-chan databases.RequestResult {
	return c.db.Get(ctx, "Customer", key)
}

func (c *CustomerStore) Delete(ctx *asyncdb.ConnectionContext, key models.CustomerPK) <-chan databases.RequestResult {
	return c.db.Delete(ctx, "Customer", key)
}

func NewCustomerStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.Customer, models.CustomerPK] {
	return &CustomerStore{db: db, l: l}
}
