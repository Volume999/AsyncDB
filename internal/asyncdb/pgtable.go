package asyncdb

type PgTableFactory struct {
}

func NewPgTableFactory() *PgTableFactory {
	return &PgTableFactory{}
}

func (f *PgTableFactory) CreateTable(name string) (Table, error) {
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
