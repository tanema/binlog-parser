package parser

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/siddontang/go-mysql/replication"

	"github.com/tanema/binlog-parser/src/database"
)

func TestConvertQueryEventToMessage(t *testing.T) {
	logPos := uint32(100)
	query := "SELECT 1"

	eventHeader := replication.EventHeader{Timestamp: uint32(time.Now().Unix()), LogPos: logPos}
	queryEvent := replication.QueryEvent{Query: []byte(query)}

	message := ConvertQueryEventToMessage(eventHeader, queryEvent)

	assertMessageHeader(t, message, logPos, MessageTypeQuery)

	if string(message.(QueryMessage).Query) != query {
		t.Fatal("Unexpected value for query ")
	}
}

func TestConvertRowsEventsToMessages(t *testing.T) {
	logPos := uint32(100)
	xid := uint64(200)

	tableMetadata := database.TableMetadata{
		Schema: "db_name",
		Table:  "table_name",
		Fields: []string{"field_1", "field_2"},
	}

	testCasesWriteRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.WRITE_ROWS_EVENTv1},
		{replication.WRITE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesWriteRowsEvents {
		t.Run("Insert message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xid, rowsEventData)

			if len(convertedMessages) != 2 {
				t.Fatal("Expected 2 insert messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, MessageTypeInsert)
			assertMessageHeader(t, convertedMessages[1], logPos, MessageTypeInsert)

			insertMessageOne := convertedMessages[0].(InsertMessage)

			if !reflect.DeepEqual(insertMessageOne.Data, MessageRowData{Row: MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for insert message 1 - got %v", insertMessageOne.Data))
			}

			insertMessageTwo := convertedMessages[1].(InsertMessage)

			if !reflect.DeepEqual(insertMessageTwo.Data, MessageRowData{Row: MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for insert message 2 - got %v", insertMessageTwo.Data))
			}
		})
	}

	testCasesDeleteRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.DELETE_ROWS_EVENTv1},
		{replication.DELETE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesDeleteRowsEvents {
		t.Run("Delete message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xid, rowsEventData)

			if len(convertedMessages) != 2 {
				t.Fatal("Expected 2 delete messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, MessageTypeDelete)
			assertMessageHeader(t, convertedMessages[1], logPos, MessageTypeDelete)

			deleteMessageOne := convertedMessages[0].(DeleteMessage)

			if !reflect.DeepEqual(deleteMessageOne.Data, MessageRowData{Row: MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for delete message 1 - got %v", deleteMessageOne.Data))
			}

			deleteMessageTwo := convertedMessages[1].(DeleteMessage)

			if !reflect.DeepEqual(deleteMessageTwo.Data, MessageRowData{Row: MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for delete message 2 - got %v", deleteMessageTwo.Data))
			}
		})
	}

	testCasesUpdateRowsEvents := []struct {
		eventType replication.EventType
	}{
		{replication.UPDATE_ROWS_EVENTv1},
		{replication.UPDATE_ROWS_EVENTv2},
	}

	for _, tc := range testCasesUpdateRowsEvents {
		t.Run("Update message", func(t *testing.T) {
			eventHeader := createEventHeader(logPos, tc.eventType)
			rowsEvent := createRowsEvent([]interface{}{"value_1", "value_2"}, []interface{}{"value_3", "value_4"})
			rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

			convertedMessages := ConvertRowsEventsToMessages(xid, rowsEventData)

			if len(convertedMessages) != 1 {
				t.Fatal("Expected 1 update messages to be created")
			}

			assertMessageHeader(t, convertedMessages[0], logPos, MessageTypeUpdate)

			updateMessage := convertedMessages[0].(UpdateMessage)

			if !reflect.DeepEqual(updateMessage.OldData, MessageRowData{Row: MessageRow{"field_1": "value_1", "field_2": "value_2"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for update message old data - got %v", updateMessage.OldData))
			}

			if !reflect.DeepEqual(updateMessage.NewData, MessageRowData{Row: MessageRow{"field_1": "value_3", "field_2": "value_4"}}) {
				t.Fatal(fmt.Sprintf("Wrong data for update message new data - got %v", updateMessage.NewData))
			}
		})
	}

	t.Run("Unknown event type", func(t *testing.T) {
		eventHeader := createEventHeader(logPos, replication.RAND_EVENT) // can be any unkown event actually
		rowsEvent := createRowsEvent()
		rowsEventData := []RowsEventData{NewRowsEventData(eventHeader, rowsEvent, tableMetadata)}

		convertedMessages := ConvertRowsEventsToMessages(xid, rowsEventData)

		if len(convertedMessages) != 0 {
			t.Fatal("Expected no messages to be created from unknown event")
		}
	})
}

func TestDetectMismatch(t *testing.T) {
	t.Run("No mismatch, empty input", func(t *testing.T) {
		row := []interface{}{}
		columnNames := []string{}
		if detected, _ := detectMismatch(row, columnNames); detected {
			t.Fatal("Expected no mismatch to be detected")
		}
	})

	t.Run("No mismatch", func(t *testing.T) {
		row := []interface{}{"value 1", "value 2"}
		columnNames := []string{"field_1", "field_2"}
		if detected, _ := detectMismatch(row, columnNames); detected {
			t.Fatal("Expected no mismatch to be detected")
		}
	})

	t.Run("Detect mismatch, row is missing field", func(t *testing.T) {
		row := []interface{}{"value 1"}
		columnNames := []string{"field_1", "field_2"}
		detected, notice := detectMismatch(row, columnNames)
		if !detected {
			t.Fatal("Expected mismatch to be detected")
		}
		if !strings.Contains(notice, "row is missing field(s)") {
			t.Fatal("Wrong notice")
		}
	})

	t.Run("Detect mismatch, column name is missing field", func(t *testing.T) {
		row := []interface{}{"value 1", "value 2"}
		columnNames := []string{"field_1"}
		detected, notice := detectMismatch(row, columnNames)
		if !detected {
			t.Fatal("Expected mismatch to be detected")
		}
		if !strings.Contains(notice, "column names array is missing field(s)") {
			t.Fatal("Wrong notice")
		}
	})
}

func createEventHeader(logPos uint32, eventType replication.EventType) replication.EventHeader {
	return replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		EventType: eventType,
		LogPos:    logPos,
	}
}

func createRowsEvent(rowData ...[]interface{}) replication.RowsEvent {
	return replication.RowsEvent{Rows: rowData}
}

func assertMessageHeader(t *testing.T, message Message, expectedLogPos uint32, expectedType MessageType) {
	if message.GetHeader().BinlogPosition != expectedLogPos {
		t.Fatal("Unexpected value for BinlogPosition")
	}

	if message.GetType() != expectedType {
		t.Fatal("Unexpected value for message type")
	}
}
