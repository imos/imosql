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
	Id     int       `sql:"test_id"`
	String string    `sql:"test_string"`
	Int    int64     `sql:"test_int"`
	Time   time.Time `sql:"test_time"`
}

func checkInterfaceEqual(t *testing.T, expected string, actual interface{}) {
	var expectedInterface interface{}
	if err := json.Unmarshal([]byte(expected), &expectedInterface); err != nil {
		t.Fatalf("failed to decode an expected value: %s", err)
	}
	actualJson, err := json.Marshal(actual)
	if err != nil {
		t.Fatalf("failed to encode an actual value: %s", err)
	}
	var actualInterface interface{}
	if err := json.Unmarshal(actualJson, &actualInterface); err != nil {
		t.Fatalf("failed to decode an actual value: %s", err)
	}
	if !reflect.DeepEqual(expectedInterface, actualInterface) {
		t.Errorf("expected: %#v, actual: %#v.", expectedInterface, actualInterface)
	}
}

func TestCheckInterfaceEqual(t *testing.T) {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		t.Errorf("failed to LoadLocation: %s", err)
	}
	rows := []TestRow{
		TestRow{
			Id: 2, String: "bar", Int: 2,
			Time: time.Date(2001, 2, 3, 4, 5, 6, 0, location),
		},
	}
	checkInterfaceEqual(
		t,
		`[{"Id": 2, "String": "bar", "Int": 2, "Time": "2001-02-03T04:05:06Z"}]`,
		rows)
	if t.Failed() {
		t.Fatalf("this test must pass.")
	}
	rows[0].Time = time.Date(2002, 2, 3, 4, 5, 6, 0, location)
	childTest := testing.T{}
	checkInterfaceEqual(
		&childTest,
		`[{"Id": 2, "String": "bar", "Int": 2, "Time": "2001-02-03T04:05:06Z"}]`,
		rows)
	if !childTest.Failed() {
		t.Fatalf("this test must not pass.")
	}
}

func TestRows(t *testing.T) {
	openDatabase()
	if db == nil {
		return
	}
	rows := []TestRow{}
	db.RowsOrDie(&rows, "SELECT * FROM test ORDER BY test_id")
	checkInterfaceEqual(
		t,
		`[{"Id": 1, "String": "foo", "Int": 1, "Time": "2000-01-01T00:00:00Z"},
		  {"Id": 2, "String": "bar", "Int": 2, "Time": "2001-02-03T04:05:06Z"},
		  {"Id": 3, "String": "foobar", "Int": 3, "Time": "0000-01-01T00:00:00Z"}]`,
		rows)
	db.RowsOrDie(&rows, "SELECT * FROM test ORDER BY test_id DESC")
	checkInterfaceEqual(
		t,
		`[{"Id": 3, "String": "foobar", "Int": 3, "Time": "0000-01-01T00:00:00Z"},
		  {"Id": 2, "String": "bar", "Int": 2, "Time": "2001-02-03T04:05:06Z"},
		  {"Id": 1, "String": "foo", "Int": 1, "Time": "2000-01-01T00:00:00Z"}]`,
		rows)
}
