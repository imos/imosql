package imosql_test

import (
	imosql "./"
	"encoding/json"
	"flag"
	"reflect"
	"testing"
	"time"
)

var enableIntegrationTest = flag.Bool(
	"enable_integration_test", false,
	"Enables integration test using an actual MySQL server.")

var db *imosql.Connection = nil

func openDatabase() {
	if !*enableIntegrationTest {
		return
	}
	if db == nil {
		var err error = nil
		db, err = imosql.GetMysql("root@/test")
		if err != nil {
			panic(err)
		}
	}
}

func TestConnect(t *testing.T) {
	openDatabase()
}

func TestInteger(t *testing.T) {
	openDatabase()
	if db == nil {
		return
	}
	actual := db.IntegerOrDie("SELECT 1 + 1")
	expected := int64(2)
	if expected != actual {
		t.Errorf("expected: %v, actual: %v", expected, actual)
	}
}

type TestRow struct {
	Id     *int       `sql:"test_id"`
	String *string    `sql:"test_string"`
	Int    *int64     `sql:"test_int"`
	Time   *time.Time `sql:"test_time"`
}

func TestRows(t *testing.T) {
	openDatabase()
	if db == nil {
		return
	}
	expectedData := []byte(`[
		{"Id": 1, "String": "foo", "Int": 1, "Time": "2000-01-01T00:00:00Z"},
		{"Id": 2, "String": "bar", "Int": 2, "Time": "2001-02-03T04:05:06Z"},
		{"Id": 3, "String": "foobar", "Int": 3, "Time": "0000-01-01T00:00:00Z"}]`)
	expected := []map[string]interface{}{}
	if err := json.Unmarshal(expectedData, &expected); err != nil {
		t.Fatalf("failed to decode the expected value: %s", err)
	}
	rows := []TestRow{}
	db.RowsOrDie(&rows, "SELECT * FROM test")
	actualData, err := json.Marshal(rows)
	if err != nil {
		t.Fatalf("failed to encode the actual value: %s", err)
	}
	actual := []map[string]interface{}{}
	if err := json.Unmarshal(actualData, &actual); err != nil {
		t.Fatalf("failed to decode the actual value: %s", err)
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected: %v, actual: %v.", expected, actual)
	}
}
