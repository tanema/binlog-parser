package conversion

import (
	"strings"
	"testing"
)

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
