package parser

import (
	"strings"

	"github.com/siddontang/go-mysql/replication"

	"github.com/tanema/binlog-parser/src/database"
	"github.com/tanema/binlog-parser/src/parser/conversion"
	"github.com/tanema/binlog-parser/src/parser/messages"
)

// ConsumerFunc is a function to handle each message from the binlog
type ConsumerFunc func(messages.Message) error

// ParseBinlogToMessages will parse the binlog and emit messages to the consumer
// for each message
func ParseBinlogToMessages(binlogFilename string, tableMap database.TableMap, consumer ConsumerFunc) error {
	rowRowsEventBuffer := newRowsEventBuffer()
	return replication.NewBinlogParser().ParseFile(binlogFilename, 0, func(e *replication.BinlogEvent) error {
		switch e.Header.EventType {
		case replication.QUERY_EVENT:
			queryEvent := e.Event.(*replication.QueryEvent)
			query := string(queryEvent.Query)
			if strings.ToUpper(strings.Trim(query, " ")) != "BEGIN" && !strings.HasPrefix(strings.ToUpper(strings.Trim(query, " ")), "SAVEPOINT") {
				if err := consumer(conversion.ConvertQueryEventToMessage(*e.Header, *queryEvent)); err != nil {
					return err
				}
			}
		case replication.XID_EVENT:
			xidEvent := e.Event.(*replication.XIDEvent)
			for _, message := range conversion.ConvertRowsEventsToMessages(uint64(xidEvent.XID), rowRowsEventBuffer.drain()) {
				if err := consumer(message); err != nil {
					return err
				}
			}
		case replication.TABLE_MAP_EVENT:
			tableMapEvent := e.Event.(*replication.TableMapEvent)
			schema := string(tableMapEvent.Schema)
			table := string(tableMapEvent.Table)
			tableID := uint64(tableMapEvent.TableID)
			if err := tableMap.Add(tableID, schema, table); err != nil {
				return err
			}
		case replication.WRITE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
			rowsEvent := e.Event.(*replication.RowsEvent)
			tableID := uint64(rowsEvent.TableID)
			tableMetadata, ok := tableMap.LookupTableMetadata(tableID)
			if !ok {
				return nil
			}
			rowRowsEventBuffer.bufferRowsEventData(conversion.NewRowsEventData(*e.Header, *rowsEvent, tableMetadata))
		}
		return nil
	})
}
