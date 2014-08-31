package imosql_test

import (
	"database/sql"
	"encoding/json"
	. "./"
	"reflect"
	"testing"
	"time"
)

type RowExample struct {
	Bool      bool      `sql:"row_bool"`
	BoolPtr   *bool     `sql:"row_bool_ptr"`
	Int64     int64     `sql:"row_int64"`
	Int64Ptr  *int64    `sql:"row_int64_ptr"`
	String    string    `sql:"row_string"`
	StringPtr *string   `sql:"row_string_ptr"`
	Datetime  time.Time `sql:"row_datetime"`
}

func parseField(rowReader *RowReader, input string, fieldName string) (
	output string, err error) {
	var rowReflection reflect.Value
	if input == "NULL" {
		rowReflection, err = rowReader.ParseFields([]interface{}{
			&sql.NullString{String: "", Valid: false}})
	} else {
		rowReflection, err = rowReader.ParseFields([]interface{}{
			&sql.NullString{String: input, Valid: true}})
	}
	if err != nil {
		return
	}
	byteOutput, err :=
		json.Marshal(rowReflection.Elem().FieldByName(fieldName).Interface())
	if err != nil {
		return
	}
	output = string(byteOutput)
	return
}

func testParseFields(
	t *testing.T, columnName string, fieldName string,
	testCases map[string]string) {
	rows := []RowExample{}
	rowReader, err := NewRowReader(&rows)
	if err != nil {
		t.Error("failed to create a RowReader:", err)
	}
	rowReader.SetColumns([]string{columnName})
	for input, expectedOutput := range testCases {
		output, err := parseField(rowReader, input, fieldName)
		if err != nil {
			if expectedOutput != "ERROR" {
				t.Error("failed to parse:", err)
			}
		} else {
			if output != expectedOutput {
				t.Errorf(
					"output for %#v should be %s, but %s", input, expectedOutput, output)
			}
		}
	}
}

func TestParseFields_Bool(t *testing.T) {
	testParseFields(
		t, "row_bool", "Bool",
		map[string]string{
			"0":      `false`,
			"1":      `true`,
			"-1":     `true`,
			"2":      `true`,
			"string": `true`,
			"":       `false`,
			"NULL":   `false`,
		})
}

func TestParseFields_BoolPtr(t *testing.T) {
	testParseFields(
		t, "row_bool_ptr", "BoolPtr",
		map[string]string{
			"0":      `false`,
			"1":      `true`,
			"-1":     `true`,
			"2":      `true`,
			"string": `true`,
			"":       `false`,
			"NULL":   `null`,
		})
}

func TestParseFields_Int64(t *testing.T) {
	testParseFields(
		t, "row_int64", "Int64",
		map[string]string{
			"12345":  `12345`,
			"01234":  `1234`,
			"-1234":  `-1234`,
			"string": `ERROR`,
			"":       `ERROR`,
			"NULL":   `0`,
		})
}

func TestParseFields_Int64Ptr(t *testing.T) {
	testParseFields(
		t, "row_int64_ptr", "Int64Ptr",
		map[string]string{
			"12345":  `12345`,
			"01234":  `1234`,
			"-1234":  `-1234`,
			"string": `ERROR`,
			"":       `ERROR`,
			"NULL":   `null`,
		})
}

func TestParseFields_String(t *testing.T) {
	testParseFields(
		t, "row_string", "String",
		map[string]string{
			"12345":  `"12345"`,
			"01234":  `"01234"`,
			"-1234":  `"-1234"`,
			"string": `"string"`,
			"":       `""`,
			"NULL":   `""`,
		})
}

func TestParseFields_StringPtr(t *testing.T) {
	testParseFields(
		t, "row_string_ptr", "StringPtr",
		map[string]string{
			"12345":  `"12345"`,
			"01234":  `"01234"`,
			"-1234":  `"-1234"`,
			"string": `"string"`,
			"":       `""`,
			"NULL":   `null`,
		})
}

func TestParseFields_Datetime(t *testing.T) {
	testParseFields(
		t, "row_datetime", "Datetime",
		map[string]string{
			"0000-00-00 00:00:00": `"0001-01-01T00:00:00Z"`,
			"0000-00-00 00:00:01": "ERROR",
			"0001-01-01 00:00:00": `"0001-01-01T00:00:00Z"`,
			"2001-02-03 04:05:06": `"2001-02-03T04:05:06Z"`,
			"9999-12-31 23:59:59": `"9999-12-31T23:59:59Z"`,
			"9999-99-99 99:99:99": "ERROR",
			"NULL":                `"0001-01-01T00:00:00Z"`,
		})
}
