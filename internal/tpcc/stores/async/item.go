package async

import (
	"AsyncDB/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type ItemStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func NewItemStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.Item, models.ItemPK] {
	return &ItemStore{db: db, l: l}
}

func (i *ItemStore) Put(ctx *asyncdb.ConnectionContext, value models.Item) <-chan databases.RequestResult {
	return i.db.Put(ctx, "Item", models.ItemPK{Id: value.Id}, value)
}

func (i *ItemStore) Get(ctx *asyncdb.ConnectionContext, key models.ItemPK) <-chan databases.RequestResult {
	return i.db.Get(ctx, "Item", key)
}

func (i *ItemStore) Delete(ctx *asyncdb.ConnectionContext, key models.ItemPK) <-chan databases.RequestResult {
	return i.db.Delete(ctx, "Item", key)
}
