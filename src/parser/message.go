package parser

import (
	"time"
)

// Message is the interface the encapsulates a binlog event
type Message interface {
	GetHeader() MessageHeader
	GetType() MessageType
}

// MessageType is the type of message event ENUM
type MessageType string

const (
	// MessageTypeInsert is the insert type of message
	MessageTypeInsert MessageType = "Insert"
	// MessageTypeUpdate is the update type of message
	MessageTypeUpdate MessageType = "Update"
	// MessageTypeDelete is the delete type of message
	MessageTypeDelete MessageType = "Delete"
	// MessageTypeQuery is the query type of message
	MessageTypeQuery MessageType = "Query"
)

// MessageHeader describes the origin of the message
type MessageHeader struct {
	Schema            string
	Table             string
	BinlogMessageTime string
	BinlogPosition    uint32
	XID               uint64
}

// NewMessageHeader creates and returns a new message header
func NewMessageHeader(schema string, table string, binlogMessageTime time.Time, binlogPosition uint32, xID uint64) MessageHeader {
	return MessageHeader{
		Schema:            schema,
		Table:             table,
		BinlogMessageTime: binlogMessageTime.UTC().Format(time.RFC3339),
		BinlogPosition:    binlogPosition,
		XID:               xID,
	}
}

type baseMessage struct {
	Header MessageHeader
	Type   MessageType
}

func (b baseMessage) GetHeader() MessageHeader {
	return b.Header
}

func (b baseMessage) GetType() MessageType {
	return b.Type
}

// MessageRow is the column data for a message
type MessageRow map[string]interface{}

// MessageRowData wraps MessageRow in the event that there is a mapping issue
type MessageRowData struct {
	Row           MessageRow
	MappingNotice string
}

// SQLQuery is just a plain query string
type SQLQuery string

// QueryMessage is a message that wraps a query statement
type QueryMessage struct {
	baseMessage
	Query SQLQuery
}

// NewQueryMessage creates a new query message
func NewQueryMessage(header MessageHeader, query SQLQuery) QueryMessage {
	return QueryMessage{baseMessage: baseMessage{Header: header, Type: MessageTypeQuery}, Query: query}
}

// UpdateMessage is a message that wraps a update statement
type UpdateMessage struct {
	baseMessage
	OldData MessageRowData
	NewData MessageRowData
}

// NewUpdateMessage creates a new UpdateMessage
func NewUpdateMessage(header MessageHeader, oldData MessageRowData, newData MessageRowData) UpdateMessage {
	return UpdateMessage{baseMessage: baseMessage{Header: header, Type: MessageTypeUpdate}, OldData: oldData, NewData: newData}
}

// InsertMessage is a message that wraps an insert statement
type InsertMessage struct {
	baseMessage
	Data MessageRowData
}

// NewInsertMessage creates a new insert message
func NewInsertMessage(header MessageHeader, data MessageRowData) InsertMessage {
	return InsertMessage{baseMessage: baseMessage{Header: header, Type: MessageTypeInsert}, Data: data}
}

// DeleteMessage is a message that wraps a delete statement
type DeleteMessage struct {
	baseMessage
	Data MessageRowData
}

// NewDeleteMessage creates a new delete message
func NewDeleteMessage(header MessageHeader, data MessageRowData) DeleteMessage {
	return DeleteMessage{baseMessage: baseMessage{Header: header, Type: MessageTypeDelete}, Data: data}
}
