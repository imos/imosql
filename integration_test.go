package imosql_test

import (
	imosql "./"
	"flag"
	"testing"
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
		db, err = imosql.GetMysql("root@/")
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
