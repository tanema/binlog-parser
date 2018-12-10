package parser

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/tanema/binlog-parser/src/parser/messages"
)

func TestConsumerChain(t *testing.T) {
	messageOne := messages.NewQueryMessage(
		messages.NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		messages.SQLQuery("SELECT * FROM table"),
	)

	messageTwo := messages.NewQueryMessage(
		messages.NewMessageHeader("database_name", "table_name", time.Now(), 100, 100),
		messages.SQLQuery("SELECT * FROM table"),
	)

	t.Run("No predicates, no collectors", func(t *testing.T) {
		chain := NewConsumerChain()
		if err := chain.consumeMessage(messageOne); err != nil {
			t.Fatal("Failed to consume message")
		}
	})

	t.Run("Collect as JSON file", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())
		chain := NewConsumerChain()
		chain.CollectAsJSON(tmpfile, true)
		if err := chain.consumeMessage(messageOne); err != nil {
			t.Fatal("Failed to consume message")
		}
		assertJSONOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())
		chain := NewConsumerChain()
		chain.CollectAsJSON(tmpfile, true)
		chain.IncludeSchemas([]string{"some_db", "database_name"})
		if err := chain.consumeMessage(messageTwo); err != nil {
			t.Fatal("Failed to consume message")
		}
		assertJSONOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter schema, filtered out", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())
		chain := NewConsumerChain()
		chain.CollectAsJSON(tmpfile, true)
		chain.IncludeSchemas([]string{"some_db"})
		if err := chain.consumeMessage(messageTwo); err != nil {
			t.Fatal("Failed to consume message")
		}
		assertJSONOutputEmpty(t, tmpfile)
	})

	t.Run("Filter table, passes through", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())
		chain := NewConsumerChain()
		chain.CollectAsJSON(tmpfile, true)
		chain.IncludeTables([]string{"some_table", "table_name"})
		if err := chain.consumeMessage(messageTwo); err != nil {
			t.Fatal("Failed to consume message")
		}
		assertJSONOutputNotEmpty(t, tmpfile)
	})

	t.Run("Filter table, filtered out", func(t *testing.T) {
		tmpfile, _ := ioutil.TempFile("", "messages.json")
		defer os.Remove(tmpfile.Name())
		chain := NewConsumerChain()
		chain.IncludeTables([]string{"some_table"})
		chain.CollectAsJSON(tmpfile, true)
		err := chain.consumeMessage(messageTwo)
		if err != nil {
			t.Fatal("Failed to consume message")
		}
		assertJSONOutputEmpty(t, tmpfile)
	})
}

func assertJSONOutputNotEmpty(t *testing.T, tmpfile *os.File) {
	fileContent, err := ioutil.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal("Failed to read tmp file")
	}
	if len(fileContent) == 0 {
		t.Fatal("Failed to dump JSON to file - tmp file is empty")
	}
}

func assertJSONOutputEmpty(t *testing.T, tmpfile *os.File) {
	fileContent, err := ioutil.ReadFile(tmpfile.Name())
	if err != nil {
		t.Fatal("Failed to read tmp file")
	}
	if len(fileContent) != 0 {
		t.Fatal("Expected JSON file to be empty")
	}
}
