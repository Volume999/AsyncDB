package asyncdb

import (
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type DDLSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *DDLSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	s.db = NewAsyncDB(tm, lm)
	s.ctx, _ = s.db.Connect()
}

func (s *DDLSuite) TestAsyncDB_CreateTable() {
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

func (s *DDLSuite) TestAsyncDB_CreateTable_Fails_When_DuplicateTables() {
	db := s.db
	ctx := s.ctx
	table, _ := NewGenericTable[int, int]("test")
	_ = db.CreateTable(ctx, table)
	table, _ = NewGenericTable[int, int]("test")
	err := db.CreateTable(ctx, table)
	s.EqualError(err, "table already exists - test")
}

func (s *DDLSuite) TestAsyncDB_ListTables() {
	db := s.db
	ctx := s.ctx
	table, _ := NewGenericTable[int, int]("test")
	table2, _ := NewGenericTable[int, int]("test2")
	_ = db.CreateTable(ctx, table)
	_ = db.CreateTable(ctx, table2)
	tables := db.ListTables(ctx)
	s.Contains(tables, "test")
	s.Contains(tables, "test2")
}

type DMLSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *DMLSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	s.db = NewAsyncDB(tm, lm)
	ctx, _ := s.db.Connect()
	s.ctx = ctx
	table1, _ := NewGenericTable[int, int]("test")
	table2, _ := NewGenericTable[int, int]("test2")
	_ = s.db.CreateTable(s.ctx, table1)
	_ = s.db.CreateTable(s.ctx, table2)
}

func (s *DMLSuite) TestAsyncDB_Put() {
	cases := []struct {
		name      string
		tableName string
		key       interface{}
		value     interface{}
		errorWant string
	}{
		{
			name:      "Normal Put - Should return same value",
			tableName: "test",
			key:       1,
			value:     2,
			errorWant: "",
		},
		{
			name:      "Put to non-existent table - Should Error",
			tableName: "test3",
			key:       1,
			value:     2,
			errorWant: "table not found - test3",
		},
		{
			name:      "Put to table with different key type - Should Error",
			tableName: "test",
			key:       "1",
			value:     2,
			errorWant: "type mismatch: expected key type - int, got - string",
		},
		{
			name:      "Put to table with different value type - Should Error",
			tableName: "test",
			key:       1,
			value:     "2",
			errorWant: "type mismatch: expected value type - int, got - string",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {

			db := s.db
			ctx := s.ctx
			ch := db.Put(ctx, c.tableName, c.key, c.value)
			s.Eventually(func() bool {
				select {
				case res := <-ch:
					if c.errorWant == "" {
						s.Nil(res.Err)
					} else {
						s.EqualError(res.Err, c.errorWant)
					}
					return true
				default:
					return false
				}
			}, time.Second, 100*time.Millisecond)
			if c.errorWant != "" {
				return
			}
			ch = db.Get(ctx, c.tableName, c.key)
			s.Eventually(func() bool {
				select {
				case res := <-ch:
					s.Nil(res.Err)
					s.Equal(c.value, res.Data)
					return true
				default:
					return false
				}
			}, time.Second, 100*time.Millisecond)
		})
	}
}

func (s *DMLSuite) TestAsyncDB_Put_Should_Update_Value() {
	db := s.db
	ctx := s.ctx
	<-db.Put(ctx, "test", 1, 2)
	<-db.Put(ctx, "test", 1, 3)
	ch := db.Get(ctx, "test", 1)
	s.Eventually(func() bool {
		select {
		case res := <-ch:
			s.Nil(res.Err)
			s.Equal(3, res.Data)
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)

}

func TestDDLSuite(t *testing.T) {
	suite.Run(t, new(DDLSuite))
}

func TestDMLSuite(t *testing.T) {
	suite.Run(t, new(DMLSuite))
}
