package asyncdb

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

type PgTableFactory struct {
	pool *pgxpool.Pool
}

func NewPgTableFactory(connectionString string) *PgTableFactory {
	conn, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		fmt.Errorf("failed to connect to database: %w", err)
		os.Exit(1)
	}
	return &PgTableFactory{pool: conn}
}

func (f *PgTableFactory) Close() {
	if f.pool != nil {
		f.pool.Close()
	}
}

func (f *PgTableFactory) CreateTable(name string) (Table, error) {
	ctx, _ := context.WithTimeout(context.Background(), 120*time.Second)
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key INT PRIMARY KEY, value VARCHAR(500))", name)
	_, err := f.pool.Exec(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return NewPgTable(name)
}

type PgTable struct {
}

func (p PgTable) Name() string {
	//TODO implement me
	panic("implement me")
}

func (p PgTable) Get(key interface{}) (value interface{}, err error) {
	//TODO implement me
	panic("implement me")
}

func (p PgTable) Put(key interface{}, value interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (p PgTable) Delete(key interface{}) error {
	//TODO implement me
	panic("implement me")
}

func (p PgTable) ValidateTypes(key interface{}, value interface{}) error {
	//TODO implement me
	panic("implement me")
}

func NewPgTable(name string) (*PgTable, error) {
	return &PgTable{}, nil
}
