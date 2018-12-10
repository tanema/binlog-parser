package parser

import (
	"fmt"
	"time"

	"github.com/siddontang/go-mysql/replication"

	"github.com/tanema/binlog-parser/src/database"
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
func ConvertQueryEventToMessage(binlogEventHeader replication.EventHeader, binlogEvent replication.QueryEvent) Message {
	header := NewMessageHeader(
		string(binlogEvent.Schema),
		"(unknown)",
		time.Unix(int64(binlogEventHeader.Timestamp), 0),
		binlogEventHeader.LogPos,
		0,
	)

	message := NewQueryMessage(header, SQLQuery(binlogEvent.Query))
	return Message(message)
}

// ConvertRowsEventsToMessages converts a row of binlog data into a message format
// that is consumable
func ConvertRowsEventsToMessages(xID uint64, rowsEventsData []RowsEventData) []Message {
	var ret []Message

	for _, d := range rowsEventsData {
		rowData := mapRowDataToColumnNames(d.BinlogEvent.Rows, d.TableMetadata.Fields)
		header := NewMessageHeader(
			d.TableMetadata.Schema,
			d.TableMetadata.Table,
			time.Unix(int64(d.BinlogEventHeader.Timestamp), 0),
			d.BinlogEventHeader.LogPos,
			xID,
		)

		switch d.BinlogEventHeader.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			for _, message := range createInsertMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			for _, message := range createUpdateMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			for _, message := range createDeleteMessagesFromRowData(header, rowData) {
				ret = append(ret, Message(message))
			}
		}
	}

	return ret
}

func createUpdateMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []UpdateMessage {
	if len(rowData)%2 != 0 {
		panic("update rows should be old/new pairs") // should never happen as per mysql format
	}
	ret := make([]UpdateMessage, len(rowData)/2)
	for i := 0; i < len(rowData); i += 2 {
		ret[i/2] = NewUpdateMessage(header, rowData[i], rowData[i+1])
	}
	return ret
}

func createInsertMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []InsertMessage {
	ret := make([]InsertMessage, len(rowData))
	for i, data := range rowData {
		ret[i] = NewInsertMessage(header, data)
	}
	return ret
}

func createDeleteMessagesFromRowData(header MessageHeader, rowData []MessageRowData) []DeleteMessage {
	ret := make([]DeleteMessage, len(rowData))
	for i, data := range rowData {
		ret[i] = NewDeleteMessage(header, data)
	}
	return ret
}

func mapRowDataToColumnNames(rows [][]interface{}, columnNames []string) []MessageRowData {
	var mappedRows []MessageRowData

	for _, row := range rows {
		data := make(map[string]interface{})
		unknownCount := 0

		detectedMismatch, mismatchNotice := detectMismatch(row, columnNames)
		// TODO re-query column names and update table map?

		for columnIndex, columnValue := range row {
			if detectedMismatch {
				data[fmt.Sprintf("(unknown_%d)", unknownCount)] = columnValue
				unknownCount++
			} else {
				columnName := columnNames[columnIndex]
				data[columnName] = columnValue
			}
		}

		if detectedMismatch {
			mappedRows = append(mappedRows, MessageRowData{Row: data, MappingNotice: mismatchNotice})
		} else {
			mappedRows = append(mappedRows, MessageRowData{Row: data})
		}
	}

	return mappedRows
}

func detectMismatch(row []interface{}, columnNames []string) (bool, string) {
	if len(row) > len(columnNames) {
		return true, fmt.Sprintf("column names array is missing field(s), will map them as unknown_*")
	}
	if len(row) < len(columnNames) {
		return true, fmt.Sprintf("row is missing field(s), ignoring missing")
	}
	return false, ""
}
