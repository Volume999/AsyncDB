package async

import (
	"POCS_Projects/internal/cmd/benchmark/databases"
	"POCS_Projects/internal/cmd/benchmark/databases/pocsdb"
	"POCS_Projects/internal/models"
	"log"
)

type HistoryStore struct {
	l  *log.Logger
	db *pocsdb.PocsDB
}

func NewHistoryStore(l *log.Logger, db *pocsdb.PocsDB) Store[models.History, models.HistoryPK] {
	return &HistoryStore{db: db, l: l}
}

func (i *HistoryStore) Put(ctx *pocsdb.ConnectionContext, value models.History) <-chan databases.RequestResult {
	// History is not used in the benchmark, and it does not have a primary key
	panic("implement me")
}

func (i *HistoryStore) Get(ctx *pocsdb.ConnectionContext, key models.HistoryPK) <-chan databases.RequestResult {
	return i.db.Get(ctx, models.Item{}, key)
}

func (i *HistoryStore) Delete(ctx *pocsdb.ConnectionContext, key models.HistoryPK) <-chan databases.RequestResult {
	return i.db.Delete(ctx, models.Item{}, key)
}
