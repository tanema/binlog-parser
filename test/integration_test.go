package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"database/sql"
	_ "github.com/go-sql-driver/mysql" // support mysql
	"github.com/ory/dockertest"

	"github.com/tanema/binlog-parser/src/database"
	"github.com/tanema/binlog-parser/src/parser"
)

var dataDir = filepath.Join(".", "data")
var connStr string

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	resource, err := pool.Run("mysql", "latest", []string{"MYSQL_ROOT_PASSWORD=secret"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	var bootstrap func() error
	connStr, bootstrap = databaseBootstrap(resource)
	if err := pool.Retry(bootstrap); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}
	code := m.Run()
	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	os.Exit(code)
}

func TestParseBinlogFile(t *testing.T) {
	createConsumerChain := func(stream io.Writer) parser.ConsumerChain {
		chain := parser.NewConsumerChain()
		chain.CollectAsJSON(stream, true)
		return chain
	}

	t.Run("binlog file not found", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "test")
		defer os.RemoveAll(tmpfile.Name())
		if err := parseBinlogFile("/not/there", connStr+"test_db", createConsumerChain(tmpfile)); err == nil {
			t.Fatal("Expected error when parsing non-existing file")
		}
	})

	testCases := []struct {
		fixtureFilename  string
		expectedJSONFile string
		includeTables    []string
		includeSchemas   []string
	}{
		{"fixtures/mysql-bin.01", "fixtures/01.json", nil, nil},                                  // inserts and updates
		{"fixtures/mysql-bin.02", "fixtures/02.json", nil, nil},                                  // create table, insert
		{"fixtures/mysql-bin.03", "fixtures/03.json", nil, nil},                                  // insert 2 rows, update 2 rows, update 3 rows
		{"fixtures/mysql-bin.04", "fixtures/04.json", nil, nil},                                  // large insert (1000)
		{"fixtures/mysql-bin.05", "fixtures/05.json", nil, nil},                                  // DROP TABLE ... queries only
		{"fixtures/mysql-bin.06", "fixtures/06.json", nil, nil},                                  // table schema doesn't match anymore
		{"fixtures/mysql-bin.07", "fixtures/07.json", nil, nil},                                  // mariadb format, create table, insert two rows
		{"fixtures/mysql-bin.01", "fixtures/01-include-table.json", []string{"buildings"}, nil},  // include tables
		{"fixtures/mysql-bin.01", "fixtures/01-no-events.json", []string{"unknown_table"}, nil},  // only unknown table is included - no events parsed
		{"fixtures/mysql-bin.01", "fixtures/01.json", nil, []string{"test_db"}},                  // inlcude schemas
		{"fixtures/mysql-bin.01", "fixtures/01-no-events.json", nil, []string{"unknown_schema"}}, // only unknown schema is included - no events parsed
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Parse binlog %s", tc.fixtureFilename), func(t *testing.T) {
			var buffer bytes.Buffer
			binlogFilename := filepath.Join(dataDir, tc.fixtureFilename)
			chain := createConsumerChain(&buffer)
			chain.IncludeTables(tc.includeTables)
			chain.IncludeSchemas(tc.includeSchemas)
			if err := parseBinlogFile(binlogFilename, connStr+"test_db", chain); err != nil {
				t.Fatal(fmt.Sprintf("Expected no error when successfully parsing file %s", err))
			}
			assertJSON(t, buffer, filepath.Join(dataDir, tc.expectedJSONFile))
		})
	}
}

func assertJSON(t *testing.T, buffer bytes.Buffer, expectedJSONFile string) {
	expectedJSON, err := ioutil.ReadFile(expectedJSONFile)

	if err != nil {
		t.Fatal(fmt.Sprintf("Failed to open expected JSON file: %s", err))
	}

	expected := strings.TrimSpace(string(expectedJSON))
	actual := strings.TrimSpace(buffer.String())

	if expected != actual {
		errorMessage := fmt.Sprintf(
			"JSON file %s does not match\nExpected:\n==========\n%s\n==========\nActual generated:\n%s\n==========",
			expectedJSONFile,
			expected,
			actual,
		)

		t.Fatal(errorMessage)
	}
}

func parseBinlogFile(binlogFilename, dbDsn string, consumerChain parser.ConsumerChain) error {
	db, err := database.GetDatabaseInstance(dbDsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return parser.ParseBinlog(binlogFilename, db, consumerChain)
}

func databaseBootstrap(resource *dockertest.Resource) (string, func() error) {
	port := resource.GetPort("3306/tcp")
	connStr := fmt.Sprintf("root:secret@(localhost:%s)/", port)
	return connStr, func() error {
		db, err := sql.Open("mysql", connStr)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}
		sqlFile, _ := os.Open(filepath.Join(dataDir, "fixtures", "test_db.sql"))
		reader := bufio.NewReader(sqlFile)
		defer sqlFile.Close()
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err == io.EOF {
				break
			}
			if _, err = db.Exec(line); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestLookupTableMetadata(t *testing.T) {
	db, err := database.GetDatabaseInstance(connStr + "test_db")
	if err != nil {
		t.Fatal("Could not get database")
	}
	defer db.Close()

	t.Run("Found", func(t *testing.T) {
		assertTableMetadata(t, db, 1063, "test_db", "buildings")
		assertTableMetadata(t, db, 1067, "test_db", "rooms")
	})

	t.Run("Fields", func(t *testing.T) {
		tableMetadata, ok := db.Map.LookupTableMetadata(1063)
		if ok != true {
			t.Fatal("Expected table metadata to be found")
		}
		expectedFields := []string{"building_no", "building_name", "address"}
		if !reflect.DeepEqual(tableMetadata.Fields, expectedFields) {
			t.Fatal("Wrong fields in table metadata")
		}
	})
	t.Run("Not Found", func(t *testing.T) {
		if _, ok := db.Map.LookupTableMetadata(999); ok != false {
			t.Fatal("Expected table metadata not to be found")
		}
	})
}

func assertTableMetadata(t *testing.T, db *database.DB, tableid uint64, expectedSchema string, expectedTable string) {
	tableMetadata, ok := db.Map.LookupTableMetadata(tableid)

	if ok != true {
		t.Fatal(fmt.Sprintf("metadata not found for table id %d", tableid))
	}

	if tableMetadata.Schema != expectedSchema {
		t.Fatal(fmt.Sprintf("wrong schema name for table id %d", tableid))
	}

	if tableMetadata.Table != expectedTable {
		t.Fatal(fmt.Sprintf("wrong table name for table id %d", tableid))
	}
}
