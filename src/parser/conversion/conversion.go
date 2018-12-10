package conversion

import (
	"time"

	"github.com/siddontang/go-mysql/replication"

	"github.com/tanema/binlog-parser/src/database"
	"github.com/tanema/binlog-parser/src/parser/messages"
)

// RowsEventData encapsulates a single binlog event
type RowsEventData struct {
	BinlogEventHeader replication.EventHeader
	BinlogEvent       replication.RowsEvent
	TableMetadata     database.TableMetadata
}

// NewRowsEventData creates a new RowEventData
func NewRowsEventData(binlogEventHeader replication.EventHeader, binlogEvent replication.RowsEvent, tableMetadata database.TableMetadata) RowsEventData {
	return RowsEventData{
		BinlogEventHeader: binlogEventHeader,
		BinlogEvent:       binlogEvent,
		TableMetadata:     tableMetadata,
	}
}

// ConvertQueryEventToMessage converts a query event into a message
func ConvertQueryEventToMessage(binlogEventHeader replication.EventHeader, binlogEvent replication.QueryEvent) messages.Message {
	header := messages.NewMessageHeader(
		string(binlogEvent.Schema),
		"(unknown)",
		time.Unix(int64(binlogEventHeader.Timestamp), 0),
		binlogEventHeader.LogPos,
		0,
	)

	message := messages.NewQueryMessage(header, messages.SQLQuery(binlogEvent.Query))
	return messages.Message(message)
}

// ConvertRowsEventsToMessages converts a row of binlog data into a message format
// that is consumable
func ConvertRowsEventsToMessages(xID uint64, rowsEventsData []RowsEventData) []messages.Message {
	var ret []messages.Message

	for _, d := range rowsEventsData {
		rowData := mapRowDataToColumnNames(d.BinlogEvent.Rows, d.TableMetadata.Fields)
		header := messages.NewMessageHeader(
			d.TableMetadata.Schema,
			d.TableMetadata.Table,
			time.Unix(int64(d.BinlogEventHeader.Timestamp), 0),
			d.BinlogEventHeader.LogPos,
			xID,
		)

		switch d.BinlogEventHeader.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			for _, message := range createInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			for _, message := range createUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			for _, message := range createDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, messages.Message(message))
			}
		}
	}

	return ret
}

func createUpdateMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.UpdateMessage {
	if len(rowData)%2 != 0 {
		panic("update rows should be old/new pairs") // should never happen as per mysql format
	}
	ret := make([]messages.UpdateMessage, len(rowData)/2)
	for i := 0; i < len(rowData); i += 2 {
		ret[i/2] = messages.NewUpdateMessage(header, rowData[i], rowData[i+1])
	}
	return ret
}

func createInsertMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.InsertMessage {
	ret := make([]messages.InsertMessage, len(rowData))
	for i, data := range rowData {
		ret[i] = messages.NewInsertMessage(header, data)
	}
	return ret
}

func createDeleteMessagesFromRowData(header messages.MessageHeader, rowData []messages.MessageRowData) []messages.DeleteMessage {
	ret := make([]messages.DeleteMessage, len(rowData))
	for i, data := range rowData {
		ret[i] = messages.NewDeleteMessage(header, data)
	}
	return ret
}
