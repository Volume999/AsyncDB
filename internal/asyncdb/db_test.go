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
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
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

func (s *DDLSuite) TestAsyncDB_DeleteTable() {
	cases := []struct {
		name            string
		tableName       string
		deleteTableName string
		errorWant       string
	}{
		{
			name:            "Normal Delete - Should not Error",
			tableName:       "test",
			deleteTableName: "test",
			errorWant:       "",
		},
		{
			name:            "Delete non-existent table - Should Error",
			tableName:       "test",
			deleteTableName: "test2",
			errorWant:       "table not found - test2",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			db := s.db
			ctx := s.ctx
			table, _ := NewGenericTable[int, int](c.tableName)
			_ = db.CreateTable(ctx, table)
			err := db.DropTable(ctx, c.deleteTableName)
			if c.errorWant == "" {
				s.Nil(err)
			} else {
				s.EqualError(err, c.errorWant)
			}
		})
	}
}

type TCLSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *TCLSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
	s.ctx, _ = s.db.Connect()
}

func (s *TCLSuite) TestAsyncDB_BeginTransaction() {
	db := s.db
	ctx := s.ctx
	err := db.BeginTransaction(ctx)
	s.Nil(err)
}

func (s *TCLSuite) TestAsyncDB_CommitTransaction() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	err := db.CommitTransaction(ctx)
	s.Nil(err)
}

type DMLSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *DMLSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
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

func (s *DMLSuite) TestAsyncDB_Get() {
	cases := []struct {
		name         string
		putTableName string
		putKey       interface{}
		putValue     interface{}
		getTableName string
		getKey       interface{}
		errorWant    string
	}{
		{
			name:         "Normal Get - Should return same value",
			putTableName: "test",
			putKey:       1,
			putValue:     2,
			getTableName: "test",
			getKey:       1,
			errorWant:    "",
		},
		{
			name:         "Get from non-existent table - Should Error",
			putTableName: "test",
			putKey:       1,
			putValue:     2,
			getTableName: "test3",
			getKey:       1,
			errorWant:    "table not found - test3",
		},
		{
			name:         "Get non-existent key - Should Error",
			putTableName: "test",
			putKey:       1,
			putValue:     2,
			getTableName: "test",
			getKey:       2,
			errorWant:    "key not found - 2",
		},
		{
			name:         "Get from table with different key type - Should Error",
			putTableName: "test",
			putKey:       1,
			putValue:     2,
			getTableName: "test",
			getKey:       "1",
			errorWant:    "type mismatch: expected key type - int, got - string",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			db := s.db
			ctx := s.ctx
			<-db.Put(ctx, c.putTableName, c.putKey, c.putValue)
			ch := db.Get(ctx, c.getTableName, c.getKey)
			s.Eventually(func() bool {
				select {
				case res := <-ch:
					if c.errorWant == "" {
						s.Nil(res.Err)
						s.Equal(c.putValue, res.Data)
					} else {
						s.EqualError(res.Err, c.errorWant)
					}
					return true
				}
			}, time.Second, 100*time.Millisecond)
		})
	}
}

func (s *DMLSuite) TestAsyncDB_Delete() {
	cases := []struct {
		name            string
		putTableName    string
		putKey          interface{}
		putValue        interface{}
		deleteTableName string
		deleteKey       interface{}
		getErrorWant    string
		errorWant       string
	}{
		{
			name:            "Normal Delete - Get should return error",
			putTableName:    "test",
			putKey:          1,
			putValue:        2,
			deleteTableName: "test",
			deleteKey:       1,
			getErrorWant:    "key not found - 1",
			errorWant:       "",
		},
		{
			name:            "Delete from non-existent table - Should Error",
			putTableName:    "test",
			putKey:          1,
			putValue:        2,
			deleteTableName: "test3",
			deleteKey:       1,
			getErrorWant:    "",
			errorWant:       "table not found - test3",
		},
		{
			name:            "Delete non-existent key - Should Error",
			putTableName:    "test",
			putKey:          1,
			putValue:        2,
			deleteTableName: "test",
			deleteKey:       2,
			getErrorWant:    "",
			errorWant:       "key not found - 2",
		},
		{
			name:            "Delete from table with different key type - Should Error",
			putTableName:    "test",
			putKey:          1,
			putValue:        2,
			deleteTableName: "test",
			deleteKey:       "1",
			getErrorWant:    "",
			errorWant:       "type mismatch: expected key type - int, got - string",
		},
	}
	for _, c := range cases {
		s.Run(c.name, func() {
			db := s.db
			ctx := s.ctx
			<-db.Put(ctx, c.putTableName, c.putKey, c.putValue)
			ch := db.Delete(ctx, c.deleteTableName, c.deleteKey)
			s.Eventually(func() bool {
				select {
				case res := <-ch:
					if c.errorWant == "" {
						s.Nil(res.Err)
					} else {
						s.EqualError(res.Err, c.errorWant)
					}
					return true
				}
			}, time.Second, 100*time.Millisecond)
			if c.getErrorWant != "" {
				res := <-db.Get(ctx, c.deleteTableName, c.deleteKey)
				s.EqualError(res.Err, c.getErrorWant)
			}
		})
	}
}

func (s *DMLSuite) TestAsyncDB_SimpleTransaction() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	<-db.Put(ctx, "test", 1, 2)
	<-db.Put(ctx, "test", 2, 3)
	db.CommitTransaction(ctx)
	ch := db.Get(ctx, "test", 1)
	s.Eventually(func() bool {
		select {
		case res := <-ch:
			s.Nil(res.Err)
			s.Equal(2, res.Data)
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
}

//func (s *DMLSuite) TestAsyncDB_When_Reading_Record_Written_By_Same_Transaction_Should_Return_Value() {
//	db := s.db
//	ctx := s.ctx
//	_ = db.StartTransaction(ctx)
//	<-db.Put(ctx, "test", 1, 2)
//	<-db.Put(ctx, "test", 2, 3)
//	ch := db.Get(ctx, "test", 1)
//	s.Eventually(func() bool {
//		select {
//		case res := <-ch:
//			s.Nil(res.Err)
//			s.Equal(2, res.Data)
//			return true
//		default:
//			return false
//		}
//	}, time.Second, 100*time.Millisecond)
//}

func (s *DMLSuite) TestAsyncDB_TransactionAbort_Should_Rollback() {
	db := s.db
	ctx := s.ctx
	<-db.Put(ctx, "test", 1, 2)
	_ = db.BeginTransaction(ctx)
	<-db.Put(ctx, "test", 1, 3)
	db.RollbackTransaction(ctx)
	ch := db.Get(ctx, "test", 1)
	s.Eventually(func() bool {
		select {
		case res := <-ch:
			s.Nil(res.Err)
			s.Equal(2, res.Data)
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
}

func (s *DMLSuite) TestAsyncDB_TransactionRollback_Should_Allow_New_Transaction() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	_ = db.RollbackTransaction(ctx)
	err := db.BeginTransaction(ctx)
	s.Nil(err)
}

func (s *DMLSuite) TestAsyncDB_TransactionAbort_Should_Fail_On_New_Transaction() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	_ = db.abortTransaction(ctx)
	err := db.BeginTransaction(ctx)
	s.EqualError(err, "connection in transaction")
}

func (s *DMLSuite) TestAsyncDB_BeginTransaction_Should_Fail_When_Already_In_Transaction() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	err := db.BeginTransaction(ctx)
	s.EqualError(err, "connection in transaction")
}

func (s *DMLSuite) TestAsyncDB_CommitTransaction_Should_Fail_When_Not_In_Transaction() {
	db := s.db
	ctx := s.ctx
	err := db.CommitTransaction(ctx)
	s.EqualError(err, "connection not in transaction")
}

func (s *DMLSuite) TestAsyncDB_RollbackTransaction_Should_Fail_When_Not_In_Transaction() {
	db := s.db
	ctx := s.ctx
	err := db.RollbackTransaction(ctx)
	s.EqualError(err, "connection not in transaction")
}

func TestDDLSuite(t *testing.T) {
	suite.Run(t, new(DDLSuite))
}

func TestDMLSuite(t *testing.T) {
	suite.Run(t, new(DMLSuite))
}

func TestTCLSuite(t *testing.T) {
	suite.Run(t, new(TCLSuite))
}
