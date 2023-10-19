package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type ItemStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewItemStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.Item, models.ItemPK] {
	return &ItemStore{db: db, l: l}
}

func (i *ItemStore) Put(ctx *databases.ConnectionContext, value models.Item) <-chan databases.RequestResult {
	return i.db.Put(ctx, models.Item{}, models.ItemPK{Id: value.Id}, value)
}

func (i *ItemStore) Get(ctx *databases.ConnectionContext, key models.ItemPK) <-chan databases.RequestResult {
	return i.db.Get(ctx, models.Item{}, key)
}

func (i *ItemStore) Delete(ctx *databases.ConnectionContext, key models.ItemPK) <-chan databases.RequestResult {
	return i.db.Delete(ctx, models.Item{}, key)
}
