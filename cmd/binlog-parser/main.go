package main

import (
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

func commaSeparatedListToArray(str string) []string {
	var arr []string
	for _, item := range strings.Split(str, ",") {
		item = strings.TrimSpace(item)
		if item != "" {
			arr = append(arr, item)
		}
	}
	return arr
}

func parseBinlogFile(binlogFilename, dbDsn string) error {
	chain := parser.NewConsumerChain()
	chain.CollectAsJSON(os.Stdout, *prettyPrintJSONFlag)
	chain.IncludeTables(strings.Split(*includeTablesFlag, ","))
	chain.IncludeSchemas(strings.Split(*includeSchemasFlag, ","))

	db, err := database.GetDatabaseInstance(dbDsn)
	if err != nil {
		return err
	}
	defer db.Close()

	return parser.ParseBinlog(binlogFilename, database.NewTableMap(db), chain)
}
