package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql" // support mysql
	"strings"
)

type DB struct {
	*sql.DB
	Map *TableMap
}

// GetDatabaseInstance will establish a connection and instance of the database
// you want to read
func GetDatabaseInstance(connectionString string) (*DB, error) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}

	tableMap, err := populateTableMap(db)
	if err != nil {
		return nil, err
	}

	return &DB{
		DB:  db,
		Map: tableMap,
	}, nil
}

func populateTableMap(db *sql.DB) (*TableMap, error) {
	tableInfo, err := getTableInfo(db)
	if err != nil {
		return nil, err
	}

	tableMap := newTableMap(db)
	for name, id := range tableInfo {
		nameParts := strings.Split(name, "/")
		if err := tableMap.Add(id, nameParts[0], nameParts[1]); err != nil {
			return nil, err
		}
	}

	return tableMap, nil
}

func getTableInfo(db *sql.DB) (map[string]uint64, error) {
	tableIDMap := map[string]uint64{}
	rows, err := db.Query("SELECT table_id, name FROM INFORMATION_SCHEMA.INNODB_TABLES")
	if err != nil {
		return tableIDMap, err
	}
	defer rows.Close()

	var tableName string
	var tableID uint64
	for rows.Next() {
		if err := rows.Scan(&tableID, &tableName); err != nil {
			return tableIDMap, err
		}
		tableIDMap[tableName] = tableID
	}

	return tableIDMap, nil
}

func getFieldsFromDb(db *sql.DB, schema string, table string) ([]string, error) {
	rows, err := db.Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fields := []string{}
	var columnName string
	for rows.Next() {
		if err := rows.Scan(&columnName); err != nil {
			return fields, err
		}
		fields = append(fields, columnName)
	}
	return fields, nil
}
