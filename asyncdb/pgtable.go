package asyncdb

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

type PgTableFactory struct {
	pool *pgxpool.Pool
}

func NewPgTableFactory(connectionString string) *PgTableFactory {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		fmt.Errorf("failed to parse connection string: %w", err)
		os.Exit(1)
	}
	config.MaxConns = 100
	conn, err := pgxpool.NewWithConfig(context.Background(), config)
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

func (f *PgTableFactory) GetTable(name string) (Table, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (key VARCHAR(500) PRIMARY KEY, value VARCHAR(500))", name)
	_, err := f.pool.Exec(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	return newPgTable(name, f.pool)
}

func (f *PgTableFactory) DeleteTable(name string) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
	_, err := f.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}
	return nil
}

func (f *PgTableFactory) GetExistingTables() ([]Table, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	query := "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
	rows, err := f.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get existing tables: %w", err)
	}
	defer rows.Close()
	var tables []Table
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		table, err := newPgTable(tableName, f.pool)
		if err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
		tables = append(tables, table)
	}
	return tables, nil
}

type PgTable struct {
	pool *pgxpool.Pool
	name string
}

func (p PgTable) Name() string {
	return p.name
}

func (p PgTable) Get(key interface{}) (value interface{}, err error) {
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = $1", p.name)
	keyStr := fmt.Sprintf("%v", key)
	row := p.pool.QueryRow(context.Background(), query, keyStr)
	var valueFromDb string
	err = row.Scan(&valueFromDb)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w - %v", ErrKeyNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get value from database: %w", err)
	}
	return valueFromDb, nil
}

func (p PgTable) Put(key interface{}, value interface{}) error {
	query := fmt.Sprintf("INSERT INTO %s (key, value) VALUES ($1, $2) ON CONFLICT(key) DO UPDATE SET value = $2", p.name)
	keyStr := fmt.Sprintf("%v", key)
	valStr := fmt.Sprintf("%v", value)
	_, err := p.pool.Exec(context.Background(), query, keyStr, valStr)
	if err != nil {
		return fmt.Errorf("failed to insert value into database: %w", err)
	}
	return nil
}

func (p PgTable) Delete(key interface{}) error {
	getQuery := fmt.Sprintf("SELECT value FROM %s WHERE key = $1", p.name)
	keyStr := fmt.Sprintf("%v", key)
	row := p.pool.QueryRow(context.Background(), getQuery, keyStr)
	err := row.Scan(new(string))
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w - %v", ErrKeyNotFound, key)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE key = $1", p.name)
	_, err = p.pool.Exec(context.Background(), query, keyStr)
	if err != nil {
		return fmt.Errorf("failed to delete value from database: %w", err)
	}
	return nil
}

func (p PgTable) ValidateTypes(key interface{}, value interface{}) error {
	// Interfaces will be converted to strings using fmt.Sprintf, so no need to validate types
	return nil
}

func newPgTable(name string, pool *pgxpool.Pool) (*PgTable, error) {
	return &PgTable{pool: pool, name: name}, nil
}
