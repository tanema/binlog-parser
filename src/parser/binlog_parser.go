package parser

import (
	"os"

	"github.com/tanema/binlog-parser/src/database"
	"github.com/tanema/binlog-parser/src/parser/parser"
)

// ParseBinlog sets up a consumer and parser for a binlog
func ParseBinlog(binlogFilename string, tableMap database.TableMap, consumerChain ConsumerChain) error {
	if _, err := os.Stat(binlogFilename); os.IsNotExist(err) {
		return err
	}
	return parser.ParseBinlogToMessages(binlogFilename, tableMap, consumerChain.consumeMessage)
}
