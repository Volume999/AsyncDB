package asyncdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAsyncDB_NewGenericTable_Should_Fail_When_Empty_Name(t *testing.T) {
	tableName := ""
	_, err := NewGenericTable[int, int](tableName)
	assert.EqualError(t, err, "table name cannot be empty")
}

func TestGenericTable_Name(t *testing.T) {
	tableName := "test"
	table, _ := NewGenericTable[int, int](tableName)
	got := table.Name()
	assert.Equal(t, tableName, got)
}

func TestGenericTable_Put(t *testing.T) {
	cases := []struct {
		name      string
		key       interface{}
		value     interface{}
		errorWant string
	}{
		{
			name:      "Normal Insert",
			key:       1,
			value:     2,
			errorWant: "",
		},
		{
			name:      "Type Mismatch on Key",
			key:       "1",
			value:     2,
			errorWant: "type mismatch: expected key type - int, got - string",
		},
		{
			name:      "Type Mismatch on Value",
			key:       1,
			value:     "2",
			errorWant: "type mismatch: expected value type - int, got - string",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table, _ := NewGenericTable[int, int]("test")
			err := table.Put(c.key, c.value)
			if c.errorWant != "" {
				assert.EqualError(t, err, c.errorWant)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestGenericTable_Get(t *testing.T) {
	cases := []struct {
		name      string
		putKey    interface{}
		putValue  interface{}
		getKey    interface{}
		want      interface{}
		errorWant string
	}{
		{
			name:      "Should Get Value",
			putKey:    1,
			putValue:  2,
			getKey:    1,
			want:      2,
			errorWant: "",
		},
		{
			name:      "Key Not Found",
			putKey:    1,
			putValue:  2,
			getKey:    2,
			want:      nil,
			errorWant: "key not found - 2",
		},
		{
			name:      "Type Mismatch on Key",
			putKey:    1,
			putValue:  2,
			getKey:    "1",
			want:      nil,
			errorWant: "type mismatch: expected key type - int, got - string",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table, _ := NewGenericTable[int, int]("test")
			table.Put(c.putKey, c.putValue)
			val, err := table.Get(c.getKey)
			if c.errorWant != "" {
				assert.EqualError(t, err, c.errorWant)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, val, c.putValue)
			}
		})
	}
}
