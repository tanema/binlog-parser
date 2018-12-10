package parser

import (
	"github.com/tanema/binlog-parser/src/parser/conversion"
)

type rowsEventBuffer struct {
	buffered []conversion.RowsEventData
}

func newRowsEventBuffer() rowsEventBuffer {
	return rowsEventBuffer{}
}

func (mb *rowsEventBuffer) bufferRowsEventData(d conversion.RowsEventData) {
	mb.buffered = append(mb.buffered, d)
}

func (mb *rowsEventBuffer) drain() []conversion.RowsEventData {
	ret := mb.buffered
	mb.buffered = nil

	return ret
}
