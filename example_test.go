package imosql_test

import (
	imosql "./"
	"fmt"
	"log"
)

func ExampleOpen() {
	// This is an example using github.com/go-sql-driver/mysql.  DataSourceName is
	// dependent of a SQL driver, so please consult your SQL driver's document.
	con, err := imosql.Open("mysql", "user:password@tcp(host:port)/database")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1 + 1 = %d\n", con.IntegerOrDie("SELECT 1 + 1"))
}
