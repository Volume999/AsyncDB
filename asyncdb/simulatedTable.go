package asyncdb

import "time"

// Simulated table is a table that simulates DB access, while also providing a no-contention environment.
type SimulatedTable struct {
	name         string
	accessTimeMs int
}

func NewSimulatedTable(name string, accessTimeMs int) *SimulatedTable {
	return &SimulatedTable{
		name:         name,
		accessTimeMs: accessTimeMs,
	}
}

func (s SimulatedTable) Name() string {
	return s.name
}

func (s SimulatedTable) simulateWork() {
	time.Sleep(time.Duration(s.accessTimeMs) * time.Millisecond)
}

func (s SimulatedTable) Get(key interface{}) (value interface{}, err error) {
	s.simulateWork()
	return "value", nil
}

func (s SimulatedTable) Put(key interface{}, value interface{}) error {
	s.simulateWork()
	return nil
}

func (s SimulatedTable) Delete(key interface{}) error {
	s.simulateWork()
	return nil
}

func (s SimulatedTable) ValidateTypes(key interface{}, value interface{}) error {
	return nil
}
