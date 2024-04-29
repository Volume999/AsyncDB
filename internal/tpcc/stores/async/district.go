package async

import (
	"github.com/Volume999/AsyncDB/asyncdb"
	"github.com/Volume999/AsyncDB/internal/databases"
	"github.com/Volume999/AsyncDB/internal/tpcc/models"
	"log"
)

type DisctrictStore struct {
	l  *log.Logger
	db *asyncdb.AsyncDB
}

func NewDiscrictStore(l *log.Logger, db *asyncdb.AsyncDB) Store[models.District, models.DistrictPK] {
	return &DisctrictStore{db: db, l: l}
}

func (d *DisctrictStore) Put(ctx *asyncdb.ConnectionContext, value models.District) <-chan databases.RequestResult {
	return d.db.Put(ctx, "District", models.DistrictPK{Id: value.Id}, value)
}

func (d *DisctrictStore) Get(ctx *asyncdb.ConnectionContext, key models.DistrictPK) <-chan databases.RequestResult {
	return d.db.Get(ctx, "District", key)
}

func (d *DisctrictStore) Delete(ctx *asyncdb.ConnectionContext, key models.DistrictPK) <-chan databases.RequestResult {
	return d.db.Delete(ctx, "District", key)
}
