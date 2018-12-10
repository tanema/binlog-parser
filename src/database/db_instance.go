package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql" // support mysql
)

// GetDatabaseInstance will establish a connection and instance of the database
// you want to read
func GetDatabaseInstance(connectionString string) (db *sql.DB, err error) {
	if db, err = sql.Open("mysql", connectionString); err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

// func getSchemas(db *sql.DB) map[string] {
//	rows, err := db.Query("SHOW SCHEMAS")
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()

//	// SELECT TABLE_ID, NAME FROM INFORMATION_SCHEMA.INNODB_TABLES
// }

// func listTables(db *sql.DB, schema string) (map[string]string, error) {
//	_, err := db.Exec("USE ?", schema)
//	if err != nil {
//	}

//	db.Query("SHOW TABLES")
//	return []string{}
// }

// func getFieldsFromDb(db *sql.DB, schema string, table string) ([]string, error) {
//	rows, err := db.Query("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION", schema, table)
//	if err != nil {
//		return nil, err
//	}
//	defer rows.Close()
//	fields := []string{}
//	var columnName string
//	for rows.Next() {
//		if err := rows.Scan(&columnName); err != nil {
//			return fields, err
//		}
//		fields = append(fields, columnName)
//	}
//	return fields, nil
// }
