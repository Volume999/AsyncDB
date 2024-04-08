package asyncdb

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type AsyncDBSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *AsyncDBSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	s.db = NewAsyncDB(tm, lm)
	s.ctx, _ = s.db.Connect()
}

func (s *AsyncDBSuite) TestAsyncDB_CreateTable() {
	cases := []struct {
		name      string
		tableName string
		errorWant string
	}{
		{
			name:      "Normal Create - Should not Error",
			tableName: "test",
			errorWant: "",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			db := s.db
			ctx := s.ctx
			table, _ := NewGenericTable[int, int](c.tableName)
			err := db.CreateTable(ctx, table)
			if c.errorWant == "" {
				s.Nil(err)
			} else {
				s.EqualError(err, c.errorWant)
			}
		})
	}
}

func (s *AsyncDBSuite) TestAsyncDB_CreateTable_Fails_When_DuplicateTables() {
	db := s.db
	ctx := s.ctx
	table, _ := NewGenericTable[int, int]("test")
	_ = db.CreateTable(ctx, table)
	table, _ = NewGenericTable[int, int]("test")
	err := db.CreateTable(ctx, table)
	s.EqualError(err, "table already exists - test")
}

func TestAsyncDBSuite(t *testing.T) {
	suite.Run(t, new(AsyncDBSuite))
}
