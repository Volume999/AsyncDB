package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type CustomerStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func (c *CustomerStore) Put(ctx *asyncdb.ConnectionContext, value models.Customer) <-chan databases.RequestResult {
	return c.db.Put(ctx, models.Customer{}, models.CustomerPK{ID: value.ID}, value)
}

func (c *CustomerStore) Get(ctx *asyncdb.ConnectionContext, key models.CustomerPK) <-chan databases.RequestResult {
	return c.db.Get(ctx, models.Customer{}, key)
}

func (c *CustomerStore) Delete(ctx *asyncdb.ConnectionContext, key models.CustomerPK) <-chan databases.RequestResult {
	return c.db.Delete(ctx, models.Customer{}, key)
}

func NewCustomerStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.Customer, models.CustomerPK] {
	return &CustomerStore{db: db, l: l}
}
