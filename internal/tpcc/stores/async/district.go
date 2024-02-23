package async

import (
	"AsyncDB/internal/asyncdb"
	"AsyncDB/internal/databases"
	"AsyncDB/internal/tpcc/models"
	"log"
)

type DisctrictStore struct {
	l  *log.Logger
	db *asyncdb.PocsDB
}

func NewDiscrictStore(l *log.Logger, db *asyncdb.PocsDB) Store[models.District, models.DistrictPK] {
	return &DisctrictStore{db: db, l: l}
}

func (d *DisctrictStore) Put(ctx *asyncdb.ConnectionContext, value models.District) <-chan databases.RequestResult {
	return d.db.Put(ctx, models.District{}, models.DistrictPK{Id: value.Id}, value)
}

func (d *DisctrictStore) Get(ctx *asyncdb.ConnectionContext, key models.DistrictPK) <-chan databases.RequestResult {
	return d.db.Get(ctx, models.District{}, key)
}

func (d *DisctrictStore) Delete(ctx *asyncdb.ConnectionContext, key models.DistrictPK) <-chan databases.RequestResult {
	return d.db.Delete(ctx, models.District{}, key)
}
