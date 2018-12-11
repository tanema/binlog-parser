package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/tanema/binlog-parser/src/database"
	"github.com/tanema/binlog-parser/src/parser"
)

var prettyPrintJSONFlag = flag.Bool("prettyprint", false, "Pretty print json")
var includeTablesFlag = flag.String("include_tables", "", "comma-separated list of tables to include")
var includeSchemasFlag = flag.String("include_schemas", "", "comma-separated list of schemas to include")

func main() {
	flag.Usage = printUsage
	flag.Parse()
	if flag.NArg() != 2 {
		printUsage()
		os.Exit(1)
	}
	if err := parseBinlogFile(flag.Arg(1), flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "Got error: %s\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	binName := path.Base(os.Args[0])
	usage := "Parse a binlog file, dump JSON to stdout. Includes options to filter by schema and table.\n" +
		"Reads from information_schema database to find out the field names for a row event.\n\n" +
		"Usage:\t%s [options ...] connectionString binlog\n\n" +
		"Options are:\n\n"
	fmt.Fprintf(os.Stderr, usage, binName)
	flag.PrintDefaults()
}

func parseBinlogFile(binlogFilename, dbDsn string) error {
	db, err := database.GetDatabaseInstance(dbDsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := os.Stat(binlogFilename); os.IsNotExist(err) {
		return err
	}

	p := parser.New(db, consume)
	p.IncludeTables(strings.Split(*includeTablesFlag, ","))
	p.IncludeSchemas(strings.Split(*includeSchemasFlag, ","))
	return p.ParseFile(binlogFilename, 0)
}

func consume(message parser.Message) error {
	json, err := marshalMessage(message)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write([]byte(fmt.Sprintf("%s\n", json)))
	return err
}

func marshalMessage(message parser.Message) ([]byte, error) {
	if *prettyPrintJSONFlag {
		return json.MarshalIndent(message, "", "    ")
	}
	return json.Marshal(message)
}
