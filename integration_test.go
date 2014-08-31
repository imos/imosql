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
		db, err = imosql.GetMysql("root@/test")
		if err != nil {
			panic(err)
		}
	}
}

func TestConnect(t *testing.T) {
	openDatabase()
	if db == nil {
		return
	}
	return
}
