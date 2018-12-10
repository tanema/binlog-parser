package database

import (
	"database/sql"
)

// TableMetadata encapsulates the column data for a table
type TableMetadata struct {
	ID     uint64
	Schema string
	Table  string
	Fields []string
}

// TableMap keeps track of the table metadata for all tables in the database
type TableMap struct {
	db      *sql.DB
	idMap   map[uint64]string
	nameMap map[string]TableMetadata
}

func newTableMap(db *sql.DB) *TableMap {
	return &TableMap{
		db:      db,
		idMap:   make(map[uint64]string),
		nameMap: make(map[string]TableMetadata),
	}
}

// Add will add the metadata for this table into the database map
func (m *TableMap) Add(id uint64, schema, table string) error {
	fields, err := getFieldsFromDb(m.db, schema, table)
	if err != nil {
		return err
	}
	name := schema + "/" + table
	m.nameMap[name] = TableMetadata{
		ID:     id,
		Schema: schema,
		Table:  table,
		Fields: fields,
	}
	m.idMap[id] = name
	return nil
}

// LookupTableMetadata will find the cached metadata for a table we are tracking
func (m *TableMap) LookupTableMetadata(id uint64) (TableMetadata, bool) {
	name, ok := m.idMap[id]
	if !ok {
		return TableMetadata{}, false
	}
	metadata, ok := m.nameMap[name]
	return metadata, ok
}
