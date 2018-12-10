package parser

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestParser(t *testing.T) {
	message := NewQueryMessage(
		NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		SQLQuery("SELECT * FROM table"),
	)

	t.Run("handles invalid predicates", func(t *testing.T) {
		p, _ := createParserWithConsumer()
		p.IncludeSchemas([]string{""})
		p.IncludeTables([]string{""})
		if len(p.predicates) > 0 {
			t.Fatal("invalid predicates")
		}
	})

	t.Run("Filter schema, passes through", func(t *testing.T) {
		p, buf := createParserWithConsumer()
		p.IncludeSchemas([]string{"some_db", "database_name"})
		p.sendMessage(message)
		if buf.String() == "" {
			t.Fatal("no output")
		}
	})

	t.Run("Filter schema, filtered out", func(t *testing.T) {
		p, buf := createParserWithConsumer()
		p.IncludeSchemas([]string{"some_db"})
		p.sendMessage(message)
		if buf.String() != "" {
			t.Fatal("unexpected output")
		}
	})

	t.Run("Filter table, passes through", func(t *testing.T) {
		p, buf := createParserWithConsumer()
		p.IncludeTables([]string{"some_table", "table_name"})
		p.sendMessage(message)
		if buf.String() == "" {
			t.Fatal("no output")
		}
	})

	t.Run("Filter table, filtered out", func(t *testing.T) {
		p, buf := createParserWithConsumer()
		p.IncludeTables([]string{"some_table"})
		p.sendMessage(message)
		if buf.String() != "" {
			t.Fatal("unexpected output")
		}
	})
}

func TestRowsEventBuffer(t *testing.T) {
	eventDataOne := RowsEventData{}
	eventDataTwo := RowsEventData{}

	t.Run("Drain Empty", func(t *testing.T) {
		buffer := rowsEventBuffer{}
		buffered := buffer.drain()

		if len(buffered) != 0 {
			t.Fatal("Wrong number of entries retrieved from empty buffer")
		}
	})

	t.Run("Drain and re-fill", func(t *testing.T) {
		buffer := rowsEventBuffer{}
		buffer.bufferRowsEventData(eventDataOne)
		buffer.bufferRowsEventData(eventDataTwo)
		buffered := buffer.drain()

		if len(buffered) != 2 {
			t.Fatal("Wrong number of entries retrieved from buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from buffer")
		}

		if !reflect.DeepEqual(buffered[1], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 1 from buffer")
		}

		buffer.bufferRowsEventData(eventDataOne)
		buffered = buffer.drain()

		if len(buffered) != 1 {
			t.Fatal("Wrong number of entries retrieved from re-used buffer")
		}

		if !reflect.DeepEqual(buffered[0], eventDataOne) {
			t.Fatal("Retrieved wrong entry at index 0 from re-used buffer")
		}
	})
}

func createParserWithConsumer() (Parser, *bytes.Buffer) {
	buf := bytes.NewBufferString("")
	p := Parser{consumer: func(mes Message) error {
		data, _ := json.Marshal(mes)
		buf.Write(data)
		return nil
	}}
	return p, buf
}
