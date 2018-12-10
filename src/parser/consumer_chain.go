package parser

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/tanema/binlog-parser/src/parser/messages"
)

// ConsumerChain is a consumer that will collect messages as json and write out
// the formatted data to a stream. It includes filtering of events
type ConsumerChain struct {
	predicates  []predicate
	collectors  []collector
	prettyPrint bool
}

type predicate func(message messages.Message) bool
type collector func(message messages.Message) error

// NewConsumerChain builds and returns a consumer chain
func NewConsumerChain() ConsumerChain {
	return ConsumerChain{}
}

// IncludeTables will set the filter for selected tables
func (c *ConsumerChain) IncludeTables(tables []string) {
	tables = clean(tables)
	if len(tables) > 0 {
		c.predicates = append(c.predicates, tablesPredicate(tables))
	}
}

// IncludeSchemas will set a filter for a selected schemas
func (c *ConsumerChain) IncludeSchemas(schemas []string) {
	schemas = clean(schemas)
	if len(schemas) > 0 {
		c.predicates = append(c.predicates, schemaPredicate(schemas))
	}
}

// CollectAsJSON adds a stream to write out the json. passing pretty print will
// set how the output is formatted
func (c *ConsumerChain) CollectAsJSON(stream io.Writer, prettyPrint bool) {
	c.collectors = append(c.collectors, streamCollector(stream, prettyPrint))
}

func (c *ConsumerChain) consumeMessage(message messages.Message) error {
	for _, predicate := range c.predicates {
		pass := predicate(message)
		if !pass {
			return nil
		}
	}

	for _, collector := range c.collectors {
		collectorErr := collector(message)
		if collectorErr != nil {
			return collectorErr
		}
	}

	return nil
}

func streamCollector(stream io.Writer, prettyPrint bool) collector {
	return func(message messages.Message) error {
		json, err := marshalMessage(message, prettyPrint)

		if err != nil {
			return err
		}

		if _, err := stream.Write([]byte(fmt.Sprintf("%s\n", json))); err != nil {
			return err
		}

		return nil
	}
}

func schemaPredicate(databases []string) predicate {
	return func(message messages.Message) bool {
		if message.GetHeader().Schema == "" {
			return true
		}
		return contains(databases, message.GetHeader().Schema)
	}
}

func tablesPredicate(tables []string) predicate {
	return func(message messages.Message) bool {
		if message.GetHeader().Table == "" {
			return true
		}

		return contains(tables, message.GetHeader().Table)
	}
}

func marshalMessage(message messages.Message, prettyPrint bool) ([]byte, error) {
	if prettyPrint {
		return json.MarshalIndent(message, "", "    ")
	}

	return json.Marshal(message)
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
