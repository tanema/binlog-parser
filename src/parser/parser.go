package parser

import (
	"os"
	"strings"

	"github.com/siddontang/go-mysql/replication"

	"github.com/tanema/binlog-parser/src/database"
)

// ConsumerFunc is a function to handle each message from the binlog
type ConsumerFunc func(Message) error
type predicate func(message Message) bool

type rowsEventBuffer struct {
	buffered []RowsEventData
}

func (mb *rowsEventBuffer) bufferRowsEventData(d RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *rowsEventBuffer) drain() []RowsEventData {
	ret := mb.buffered
	mb.buffered = nil
	return ret
}

// Parser is the object that scans the binlog, selects the messages based on
// predicates and then emits the messages
type Parser struct {
	consumer           ConsumerFunc
	rowRowsEventBuffer rowsEventBuffer
	db                 *database.DB
	predicates         []predicate
}

// New creates a new Parser for a binlog and database
func New(db *database.DB, consumer ConsumerFunc) Parser {
	return Parser{
		db:                 db,
		rowRowsEventBuffer: rowsEventBuffer{},
		consumer:           consumer,
	}
}

// IncludeTables will set the filter for selected tables
func (p *Parser) IncludeTables(tables []string) {
	tables = clean(tables)
	if len(tables) > 0 {
		tablePredicate := func(message Message) bool {
			if message.GetHeader().Table == "" {
				return true
			}
			return contains(tables, message.GetHeader().Table)
		}
		p.predicates = append(p.predicates, tablePredicate)
	}
}

// IncludeSchemas will set a filter for a selected schemas
func (p *Parser) IncludeSchemas(schemas []string) {
	schemas = clean(schemas)
	if len(schemas) > 0 {
		schemaPredicate := func(message Message) bool {
			if message.GetHeader().Schema == "" {
				return true
			}
			return contains(schemas, message.GetHeader().Schema)
		}
		p.predicates = append(p.predicates, schemaPredicate)
	}
}

// ParseFile will parse the binlog and emit messages to the consumer
// for each message
func (p *Parser) ParseFile(filename string, offset int64) error {
	return replication.NewBinlogParser().ParseFile(filename, 0, p.handleEvent)
}

func (p *Parser) handleEvent(e *replication.BinlogEvent) error {
	e.Dump(os.Stdout)
	switch e.Header.EventType {
	case replication.QUERY_EVENT:
		queryEvent := e.Event.(*replication.QueryEvent)
		query := string(queryEvent.Query)
		if strings.ToUpper(strings.Trim(query, " ")) != "BEGIN" && !strings.HasPrefix(strings.ToUpper(strings.Trim(query, " ")), "SAVEPOINT") {
			if err := p.sendMessage(ConvertQueryEventToMessage(*e.Header, *queryEvent)); err != nil {
				return err
			}
		}
	case replication.XID_EVENT:
		xidEvent := e.Event.(*replication.XIDEvent)
		for _, message := range ConvertRowsEventsToMessages(uint64(xidEvent.XID), p.rowRowsEventBuffer.drain()) {
			if err := p.sendMessage(message); err != nil {
				return err
			}
		}
	case replication.TABLE_MAP_EVENT:
		tableMapEvent := e.Event.(*replication.TableMapEvent)
		schema := string(tableMapEvent.Schema)
		table := string(tableMapEvent.Table)
		tableID := uint64(tableMapEvent.TableID)
		if err := p.db.Map.Add(tableID, schema, table); err != nil {
			return err
		}
	case replication.WRITE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		rowsEvent := e.Event.(*replication.RowsEvent)
		tableID := uint64(rowsEvent.TableID)
		tableMetadata, ok := p.db.Map.LookupTableMetadata(tableID)
		if !ok {
			return nil
		}
		p.rowRowsEventBuffer.bufferRowsEventData(NewRowsEventData(*e.Header, *rowsEvent, tableMetadata))
	}
	return nil
}

func (p *Parser) sendMessage(message Message) error {
	for _, predicate := range p.predicates {
		pass := predicate(message)
		if !pass {
			return nil
		}
	}
	return p.consumer(message)
}

func clean(items []string) (arr []string) {
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			arr = append(arr, item)
		}
	}
	return
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
