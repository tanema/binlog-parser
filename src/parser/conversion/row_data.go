package conversion

import (
	"fmt"

	"github.com/tanema/binlog-parser/src/parser/messages"
)

func mapRowDataToColumnNames(rows [][]interface{}, columnNames []string) []messages.MessageRowData {
	var mappedRows []messages.MessageRowData

	for _, row := range rows {
		data := make(map[string]interface{})
		unknownCount := 0

		detectedMismatch, mismatchNotice := detectMismatch(row, columnNames)

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
			mappedRows = append(mappedRows, messages.MessageRowData{Row: data, MappingNotice: mismatchNotice})
		} else {
			mappedRows = append(mappedRows, messages.MessageRowData{Row: data})
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
