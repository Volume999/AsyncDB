package asyncdb

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenericTable_Name(t *testing.T) {
	tableName := "test"
	table := NewGenericTable[int, int](tableName)
	got := table.Name()
	assert.Equal(t, tableName, got)
}

func TestGenericTable_Put(t *testing.T) {
	cases := []struct {
		name      string
		key       interface{}
		value     interface{}
		errorWant error
	}{
		{
			name:      "Normal Insert",
			key:       1,
			value:     2,
			errorWant: nil,
		},
		{
			name:      "Type Mismatch on Key",
			key:       "1",
			value:     2,
			errorWant: fmt.Errorf("type mismatch: expected key type - int, got - string"),
		},
		{
			name:      "Type Mismatch on Value",
			key:       1,
			value:     "2",
			errorWant: fmt.Errorf("type mismatch: expected value type - int, got - string"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table := NewGenericTable[int, int]("test")
			err := table.Put(c.key, c.value)
			if c.errorWant != nil {
				assert.NotNil(t, err)
				assert.Equal(t, c.errorWant.Error(), err.Error())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func FuzzGenericTable_Hash_NoCollisions(f *testing.F) {
	f.Fuzz(func(t *testing.T, name1 string, name2 string) {
		t1 := NewGenericTable[int, int](name1)
		t2 := NewGenericTable[int, int](name2)
		if t1.Hash() == t2.Hash() {
			assert.Equal(t, name1, name2)
		} else {
			assert.NotEqual(t, name1, name2)
		}
	})
}
