package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type HistoryStore struct {
	l  *log.Logger
	db *asyncdb.PocsDB
}

func NewHistoryStore(l *log.Logger, db *asyncdb.PocsDB) Store[models.History, models.HistoryPK] {
	return &HistoryStore{db: db, l: l}
}

func (i *HistoryStore) Put(ctx *asyncdb.ConnectionContext, value models.History) <-chan databases.RequestResult {
	// History is not used in the benchmark, and it does not have a primary key
	panic("implement me")
}

func (i *HistoryStore) Get(ctx *asyncdb.ConnectionContext, key models.HistoryPK) <-chan databases.RequestResult {
	return i.db.Get(ctx, models.Item{}, key)
}

func (i *HistoryStore) Delete(ctx *asyncdb.ConnectionContext, key models.HistoryPK) <-chan databases.RequestResult {
	return i.db.Delete(ctx, models.Item{}, key)
}
