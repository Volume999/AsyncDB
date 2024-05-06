package asyncdb

import (
	"errors"
	"fmt"
	"github.com/Volume999/AsyncDB/internal/databases"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"sync"
	"testing"
	"time"
)

const (
	TableImpl = "InMemory" // InMemory, PgTable
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
			table, _ := NewInMemoryTable[int, int](c.tableName)
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
	table, _ := NewInMemoryTable[int, int]("test")
	_ = db.CreateTable(ctx, table)
	table, _ = NewInMemoryTable[int, int]("test")
	err := db.CreateTable(ctx, table)
	s.EqualError(err, "table already exists - test")
}

func (s *DDLSuite) TestAsyncDB_ListTables() {
	db := s.db
	ctx := s.ctx
	table, _ := NewInMemoryTable[int, int]("test")
	table2, _ := NewInMemoryTable[int, int]("test2")
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
			table, _ := NewInMemoryTable[int, int](c.tableName)
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

	// PgTable implementation
	pgTableFactory *PgTableFactory
}

func (s *DMLSuite) SetupSuite() {
	//PgTable implementation
	if TableImpl == "PgTable" {
		pgTableFactory, err := NewPgTableFactory("postgres://postgres:secret@localhost:5432/postgres")
		if err != nil {
			s.T().Fatal(err)
		}
		s.pgTableFactory = pgTableFactory
	}
}

func (s *DMLSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
	ctx, _ := s.db.Connect()
	s.ctx = ctx

	// TODO: This needs to be conditional based on the suite flag
	//In-memory table implementation
	if TableImpl == "InMemory" {
		table1, _ := NewInMemoryTable[int, int]("test")
		table2, _ := NewInMemoryTable[int, int]("test2")
		_ = s.db.CreateTable(s.ctx, table1)
		_ = s.db.CreateTable(s.ctx, table2)
	} else if TableImpl == "PgTable" {
		// PgTable implementation
		table1, _ := s.pgTableFactory.GetTable("test")
		table2, _ := s.pgTableFactory.GetTable("test2")
		_ = s.db.CreateTable(s.ctx, table1)
		_ = s.db.CreateTable(s.ctx, table2)
	}
}

func (s *DMLSuite) TearDownTest() {
	//PgTable Implementation
	if TableImpl == "PgTable" {
		s.pgTableFactory.DeleteTable("test")
		s.pgTableFactory.DeleteTable("test2")
	}
}

func (s *DMLSuite) TearDownSuite() {

	// PgTable Implementation
	if TableImpl == "PgTable" {
		s.pgTableFactory.Close()
	}
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
					s.Equal(fmt.Sprintf("%v", c.value), fmt.Sprintf("%v", res.Data))
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
			s.Equal("3", fmt.Sprintf("%v", res.Data))
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
						s.Equal(fmt.Sprintf("%v", c.putValue), fmt.Sprintf("%v", res.Data))
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
			s.Equal("2", fmt.Sprintf("%v", res.Data))
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
}

func (s *DMLSuite) TestAsyncDB_When_Reading_Record_Written_By_Same_Transaction_Should_Return_Value() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	<-db.Put(ctx, "test", 1, 2)
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
			s.Equal("2", fmt.Sprintf("%v", res.Data))
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

func (s *DMLSuite) TestAsyncDB_DirtyRead() {
	db := s.db
	ctx := s.ctx
	_ = db.BeginTransaction(ctx)
	ctx2, _ := db.Connect()
	err := <-db.Put(ctx2, "test", 1, 2)
	s.T().Log(err.Err)
	syn := make(chan struct{})
	go func() {
		defer close(syn)
		ch := db.Get(ctx, "test", 1)
		s.Eventually(func() bool {
			select {
			case res := <-ch:
				s.Nil(res.Err)
				s.Equal("2", fmt.Sprintf("%v", res.Data))
				return true
			default:
				return false
			}
		}, time.Second, 100*time.Millisecond)
	}()
	db.CommitTransaction(ctx2)
	<-syn
}

func (s *DMLSuite) TestAsyncDB_NonRepeatableRead() {
	db := s.db
	ctx := s.ctx
	ctx2, _ := db.Connect()
	_ = db.BeginTransaction(ctx2)
	<-db.Put(ctx, "test", 1, 2)
	_ = db.BeginTransaction(ctx)
	val := <-db.Get(ctx, "test", 1)
	syn := make(chan struct{})
	go func() {
		defer close(syn)
		<-db.Put(ctx2, "test", 1, 3)
		db.CommitTransaction(ctx2)
	}()
	val2 := <-db.Get(ctx, "test", 1)
	db.CommitTransaction(ctx)
	s.Eventually(func() bool {
		select {
		case <-syn:
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
	s.Equal(val2.Data, val.Data)
	val3 := <-db.Get(ctx, "test", 1)
	s.Equal("3", fmt.Sprintf("%v", val3.Data))
}

func (s *DMLSuite) TestAsyncDB_Data_Consistency() {
	db := s.db
	iters := 1000
	threads := 10
	ctx_start := s.ctx
	<-db.Put(ctx_start, "test", 1, 0)
	increment := func(ctx *ConnectionContext, tableName string) error {
		val := <-db.Get(ctx, tableName, 1)
		if val.Err != nil {
			return val.Err
		}
		val = <-db.Put(ctx, tableName, 1, val.Data.(int)+1)
		return val.Err
	}
	wg := sync.WaitGroup{}
	wg.Add(threads)
	f := func() {
		defer wg.Done()
		ctx, _ := db.Connect()
		for i := 0; i < iters; i++ {
			_ = db.BeginTransaction(ctx)
			err := increment(ctx, "test")
			for errors.Is(err, ErrXactInTerminalState) || errors.Is(err, ErrLockConflict) {
				err = increment(ctx, "test")
			}
			if err != nil {
				s.T().Errorf("Error in transaction: %v", err)
				db.RollbackTransaction(ctx)
			}
			db.CommitTransaction(ctx)
		}
	}
	for i := 0; i < threads; i++ {
		go f()
	}
	waitReturn := make(chan struct{})
	go func() {
		defer close(waitReturn)
		wg.Wait()
	}()
	assert.Eventually(s.T(), func() bool {
		select {
		case <-waitReturn:
			val := <-db.Get(ctx_start, "test", 1)
			s.Nil(val.Err)
			s.Equal(threads*iters, val.Data)
			return true
		default:
			return false
		}
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *DMLSuite) TestAsyncDB_When_Lock_Conflict_Should_Abort_Transaction() {
	cases := []string{"Put", "Get", "Delete"}
	for _, c := range cases {
		s.Run(c, func() {
			db := s.db
			ctx := s.ctx
			_ = db.BeginTransaction(ctx)
			ctx2, _ := db.Connect()
			_ = db.BeginTransaction(ctx2)
			<-db.Put(ctx, "test", 1, 2)
			switch c {
			case "Put":
				<-db.Put(ctx2, "test", 1, 3)
			case "Get":
				<-db.Get(ctx2, "test", 1)
			case "Delete":
				<-db.Delete(ctx2, "test", 1)
			}
			s.EqualError(db.BeginTransaction(ctx2), "connection in transaction")
		})
	}
}

func (s *DMLSuite) TestAsyncDB_ConcurrentOperation_Should_End_When_Rollback() {
	cases := []string{"Put", "Get", "Delete"}
	for _, c := range cases {
		s.Run(c, func() {
			db := s.db
			ctx := s.ctx
			ctx2, _ := db.Connect()
			_ = db.BeginTransaction(ctx)
			time.Sleep(1 * time.Millisecond)
			_ = db.BeginTransaction(ctx2)
			<-db.Put(ctx2, "test", 1, 2)
			aborted := make(chan struct{})
			started := make(chan struct{})
			go func() {
				defer close(aborted)
				var ch <-chan databases.RequestResult
				switch c {
				case "Put":
					ch = db.Put(ctx, "test", 1, 3)
				case "Get":
					ch = db.Get(ctx, "test", 1)
				case "Delete":
					ch = db.Delete(ctx, "test", 1)
				}
				time.Sleep(1 * time.Millisecond)
				close(started)
				s.Eventually(func() bool {
					select {
					case res := <-ch:
						// There can be different errors, such as transaction aborted error, or transaction is aborting error
						return s.NotNil(res.Err)
					default:
						return false
					}
				}, time.Second, 100*time.Millisecond)
			}()
			<-started
			_ = db.RollbackTransaction(ctx)
			s.Eventually(func() bool {
				select {
				case <-aborted:
					return true
				default:
					return false
				}
			}, time.Second, 100*time.Millisecond)
			_ = db.CommitTransaction(ctx2)
		})
	}
}

func (s *DMLSuite) TestAsyncDB_When_Commit_Concurrent_Operations_Should_Finish() {
	db := s.db
	ctx := s.ctx
	//ctx, _ = db.Connect()
	_ = db.BeginTransaction(ctx)
	ctx2, _ := db.Connect()
	_ = db.BeginTransaction(ctx2)
	//<-db.Put(ctx2, "test", 1, 2)
	time.Sleep(10 * time.Millisecond)
	db.Put(ctx, "test", 1, 3)
	wait := make(chan struct{})
	time.Sleep(10 * time.Millisecond)
	go func() {
		defer close(wait)
		if err := db.CommitTransaction(ctx); err != nil {
			s.T().Error(err)
		}
	}()
	time.Sleep(10 * time.Millisecond)
	if err := db.CommitTransaction(ctx2); err != nil {
		s.T().Error(err)
	}
	<-wait
	ctx3, _ := db.Connect()
	val := <-db.Get(ctx3, "test", 1)
	data := val.Data
	s.Nil(val.Err)
	s.Equal("3", fmt.Sprintf("%v", data))
}

func (s *DMLSuite) TestAsyncDB_When_Commit_Operations_Cannot_Be_Submitted() {
	db := s.db
	db.withImplicitTxn = false
	ctx := s.ctx
	wait := make(chan struct{})
	_ = db.BeginTransaction(ctx)
	go func() {
		for {
			db.Put(ctx, "test", 1, 2)
		}
	}()
	time.Sleep(1 * time.Microsecond)
	go func() {
		defer close(wait)
		_ = db.CommitTransaction(ctx)
	}()
	s.Eventually(func() bool {
		select {
		case <-wait:
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
}

func (s *DMLSuite) TestAsyncDB_When_Rollback_Operations_Cannot_Be_Submitted() {
	db := s.db
	db.withImplicitTxn = false
	ctx := s.ctx
	wait := make(chan struct{})
	_ = db.BeginTransaction(ctx)
	go func() {
		for {
			db.Put(ctx, "test", 1, 2)
		}
	}()
	time.Sleep(1 * time.Millisecond)
	go func() {
		defer close(wait)
		_ = db.RollbackTransaction(ctx)
	}()
	s.Eventually(func() bool {
		select {
		case <-wait:
			return true
		default:
			return false
		}
	}, time.Second, 100*time.Millisecond)
}

type InMemoryTablesSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *InMemoryTablesSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
	s.ctx, _ = s.db.Connect()
	table1, _ := NewInMemoryTable[int, int]("test")
	table2, _ := NewInMemoryTable[int, int]("test2")
	_ = s.db.CreateTable(s.ctx, table1)
	_ = s.db.CreateTable(s.ctx, table2)
}

func (s *InMemoryTablesSuite) TestAsyncDB_Get() {
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

func (s *InMemoryTablesSuite) TestAsyncDB_Put() {
	cases := []struct {
		name      string
		tableName string
		key       interface{}
		value     interface{}
		errorWant string
	}{
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

func (s *InMemoryTablesSuite) TestAsyncDB_Delete() {
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

// PostgresTableSuite is not used, can be removed later if not needed
type PostgresTablesSuite struct {
	suite.Suite
	db  *AsyncDB
	ctx *ConnectionContext
}

func (s *PostgresTablesSuite) SetupTest() {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	s.db = NewAsyncDB(tm, lm, h)
	s.ctx, _ = s.db.Connect()
	pgTableFactory, err := NewPgTableFactory("postgres://postgres:secret@localhost:5432/postgres")
	if err != nil {
		s.T().Fatal(err)
	}
	table1, _ := pgTableFactory.GetTable("test")
	table2, _ := pgTableFactory.GetTable("test2")
	_ = s.db.CreateTable(s.ctx, table1)
	_ = s.db.CreateTable(s.ctx, table2)
}

func TestAsyncDB_Implicit_Transactions_Should_Fail_If_Option_Is_Disabled(t *testing.T) {
	tm := NewTransactionManager()
	lm := NewLockManager()
	h := NewStringHasher()
	db := NewAsyncDB(tm, lm, h, WithExplicitTxn())
	ctx, _ := db.Connect()
	tbl, _ := NewInMemoryTable[int, int]("test")
	_ = db.CreateTable(ctx, tbl)
	val := <-db.Put(ctx, "test", 1, 2)
	assert.EqualError(t, val.Err, "connection not in transaction")
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

func TestInMemoryTablesSuite(t *testing.T) {
	suite.Run(t, new(InMemoryTablesSuite))
}

func TestPostgresTablesSuite(t *testing.T) {
	suite.Run(t, new(PostgresTablesSuite))
}
