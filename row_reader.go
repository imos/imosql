package imosql

import (
	"database/sql"
	"reflect"
	"strconv"
	"time"
)

type RowReader struct {
	rowsPtr                 interface{}
	rowType                 reflect.Type
	columnNameToFieldIndex  map[string]int
	columns                 []string
	columnIndexToFieldIndex []int
}

func NewRowReader(rowsPtr interface{}) (rowReader *RowReader, err error) {
	if reflect.ValueOf(rowsPtr).Kind() != reflect.Ptr {
		err = errorf(
			"rowsPtr must be a pointer but %s.",
			reflect.ValueOf(rowsPtr).Kind().String())
		return
	}
	rows := reflect.ValueOf(rowsPtr).Elem()
	if rows.Kind() != reflect.Slice {
		err = errorf("rows must be a slice but %s.", rows.Kind().String())
		return
	}
	rows.SetLen(0)
	if rows.Type().Elem().Kind() != reflect.Struct {
		err = errorf(
			"rows must be a slice of a struct but a slice of %s.",
			rows.Type().Elem().Kind().String())
		return
	}
	rowType := rows.Type().Elem()
	columnNameToFieldIndex := map[string]int{}
	for fieldIndex := 0; fieldIndex < rowType.NumField(); fieldIndex++ {
		field := rowType.Field(fieldIndex)
		if field.Tag.Get("sql") == "" {
			err = errorf(
				"every field of a row struct must have a sql tag: %s", field.Name)
			return
		}
		columnNameToFieldIndex[field.Tag.Get("sql")] = fieldIndex
	}
	rowReader = &RowReader{
		rowsPtr:                rowsPtr,
		rowType:                rowType,
		columnNameToFieldIndex: columnNameToFieldIndex,
	}
	return
}

func (rr *RowReader) SetColumns(columns []string) error {
	if len(columns) == 0 {
		return errorf("# of columns must be >0.")
	}
	rr.columnIndexToFieldIndex = []int{}
	for _, columnName := range columns {
		if fieldIndex, ok := rr.columnNameToFieldIndex[columnName]; ok {
			rr.columnIndexToFieldIndex =
				append(rr.columnIndexToFieldIndex, fieldIndex)
		} else {
			rr.columnIndexToFieldIndex =
				append(rr.columnIndexToFieldIndex, -1)
		}
	}
	rr.columns = make([]string, len(columns))
	copy(rr.columns, columns)
	return nil
}

func parseField(output reflect.Value, input string) error {
	if output.Kind() == reflect.Ptr {
		if output.IsNil() {
			output.Set(reflect.New(output.Type().Elem()))
		}
		output = output.Elem()
	}
	switch output.Interface().(type) {
	case time.Time:
		if input == "0000-00-00 00:00:00" {
			input = "0001-01-01 00:00:00"
		}
		location, err := time.LoadLocation("UTC")
		if err != nil {
			return err
		}
		result, err := time.ParseInLocation("2006-01-02 15:04:05", input, location)
		if err != nil {
			return err
		}
		output.Set(reflect.ValueOf(result))
		return nil
	}
	switch output.Kind() {
	case reflect.Bool:
		if input == "0" || input == "" {
			output.SetBool(false)
		} else {
			output.SetBool(true)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intValue, err := strconv.ParseInt(input, 10, 64)
		if err != nil {
			return err
		}
		output.SetInt(intValue)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Uintptr:
		uintValue, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return err
		}
		output.SetUint(uintValue)
	case reflect.String:
		output.SetString(input)
	default:
		return errorf("unsupported type: %s.", output.Type().String())
	}
	return nil
}

func (rr *RowReader) ParseFields(fields []sql.NullString) (row reflect.Value, err error) {
	if len(fields) != len(rr.columnIndexToFieldIndex) {
		err = errorf("len(fields) != len(rr.columnIndexToFieldIndex)")
		return
	}
	row = reflect.New(rr.rowType)
	for columnIndex, fieldValue := range fields {
		if !fieldValue.Valid {
			continue
		}
		if rr.columnIndexToFieldIndex[columnIndex] < 0 {
			continue
		}
		if err = parseField(
			row.Elem().Field(rr.columnIndexToFieldIndex[columnIndex]),
			fieldValue.String); err != nil {
			return
		}
	}
	return
}

func (rr *RowReader) Read(rows *sql.Rows, limit int) error {
	if limit < -1 {
		return errorf("limit must be -1 or no less than 0: limit = %d.", limit)
	}
	numRows := 0
	if len(rr.columns) == 0 {
		return errorf("SetColumns must be called beforehand.")
	}
	fields := make([]sql.NullString, len(rr.columns))
	interfaceFields := make([]interface{}, len(fields))
	for fieldIndex, _ := range fields {
		interfaceFields[fieldIndex] = &fields[fieldIndex]
	}
	for rows.Next() {
		if numRows == limit {
			break
		}
		numRows++
		if err := rows.Scan(interfaceFields...); err != nil {
			return err
		}
		row, err := rr.ParseFields(fields)
		if err != nil {
			return err
		}
		reflect.ValueOf(rr.rowsPtr).Elem().Set(
			reflect.Append(reflect.ValueOf(rr.rowsPtr).Elem(), row.Elem()))
	}
	return nil
}
