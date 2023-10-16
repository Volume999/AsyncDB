package sqlite

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

const (
	databasePath = "internal/databases/sqlite/db"
)

type Handler struct {
	DB *sql.DB
}

func NewHandler(dbType string) (*Handler, error) {
	var db *sql.DB
	var err error
	if dbType == "in-memory" {
		db, err = sql.Open("sqlite3", ":memory:")
	} else {
		db, err = sql.Open("sqlite3", databasePath)
	}
	if err != nil {
		return nil, fmt.Errorf("error opening database, %s", err)
	}
	return &Handler{DB: db}, nil
}
