package imosql

import (
	"database/sql"
	"flag"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"time"
)

type Connection struct {
	sql *sql.DB
}

var connection *Connection = nil
var mysqlTarget = flag.String(
	"mysql", "",
	"MySQL database to connect "+
		"(e.g. <user>:<password>@tcp(<host>:<port>)/<database>). This flag "+
		"overrides the default target.")

func GetMysql(target string) (connection *Connection, err error) {
	connection = new(Connection)
	if *mysqlTarget != "" {
		connection.sql, err = sql.Open("mysql", *mysqlTarget)
	} else if target != "" {
		connection.sql, err = sql.Open("mysql", target)
	} else {
		err = Errorf("mysql flag or a default target must be specified.")
		return
	}
	if err != nil {
		err = Errorf("failed to connect to the databse: %s", err)
		return
	}
	return
}

////////////////////////////////////////////////////////////////////////////////
// No-value query functions
////////////////////////////////////////////////////////////////////////////////

func (c *Connection) Execute(query string, args ...interface{}) (result sql.Result, err error) {
	Logf("running a SQL command: %s; %v.", query, args)
	result, err = c.sql.Exec(query, args...)
	if err != nil {
		err = Errorf("failed to run a SQL command: %s", err)
		return
	}
	if IsLogging() {
		insertId, err := result.LastInsertId()
		if err == nil && insertId != 0 {
			Logf("last insert ID is %d.", insertId)
		}
		rowsAffected, err := result.RowsAffected()
		if err == nil {
			Logf("# of affected rows is %d.", rowsAffected)
		}
		err = nil
	}
	return
}

func (c *Connection) ExecuteOrDie(query string, args ...interface{}) sql.Result {
	result, err := c.Execute(query, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (c *Connection) Change(query string, args ...interface{}) error {
	result, err := c.Execute(query, args...)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return Errorf("no row was updated.")
	}
	return nil
}

func (c *Connection) ChangeOrDie(query string, args ...interface{}) {
	err := c.Change(query, args...)
	if err != nil {
		panic(err)
	}
}

////////////////////////////////////////////////////////////////////////////////
// Single-value query functions
////////////////////////////////////////////////////////////////////////////////

func (c *Connection) parseSingleValue(result interface{}, query string, args ...interface{}) error {
	Logf("running a SQL query: %s; %v.", query, args)
	rows, err := c.sql.Query(query, args...)
	if err != nil {
		return Errorf("failed to run a SQL query: %s", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Errorf("no result.")
	}
	var stringResult string
	err = rows.Scan(&stringResult)
	if err != nil {
		return Errorf("failed to scan one field: %s", err)
	}
	err = parseField(reflect.ValueOf(result), stringResult)
	if err != nil {
		return Errorf("failed to parse a field: %s", err)
	}
	return nil
}

func (c *Connection) String(query string, args ...interface{}) (result string, err error) {
	err = c.parseSingleValue(&result, query, args...)
	return
}

func (c *Connection) Integer(query string, args ...interface{}) (result int64, err error) {
	err = c.parseSingleValue(&result, query, args...)
	return
}

func (c *Connection) Time(query string, args ...interface{}) (result time.Time, err error) {
	err = c.parseSingleValue(&result, query, args...)
	return
}

func (c *Connection) StringOrDie(query string, args ...interface{}) string {
	result, err := c.String(query, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (c *Connection) IntegerOrDie(query string, args ...interface{}) int64 {
	result, err := c.Integer(query, args...)
	if err != nil {
		panic(err)
	}
	return result
}

func (c *Connection) TimeOrDie(query string, args ...interface{}) time.Time {
	result, err := c.Time(query, args...)
	if err != nil {
		panic(err)
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////
// Multiple-value query functions
////////////////////////////////////////////////////////////////////////////////

func (c *Connection) parseRows(rowsPtr interface{}, limit int, query string, args ...interface{}) error {
	rowReader, err := NewRowReader(rowsPtr)
	if err != nil {
		return Errorf("failed to create a RowReader: %s", err)
	}
	Logf("running a SQL query: %s; %v.", query, args)
	inputRows, err := c.sql.Query(query, args...)
	if err != nil {
		return Errorf("failed to run a SQL query: %s", err)
	}
	defer inputRows.Close()
	columns, err := inputRows.Columns()
	if err != nil {
		return Errorf("failed to get columns: %s", err)
	}
	if len(columns) == 0 {
		return Errorf("no columns.")
	}
	rowReader.SetColumns(columns)
	if err := rowReader.Read(inputRows, limit); err != nil {
		return Errorf("failed to read rows: %s", err)
	}
	return nil
}

func (c *Connection) Rows(rowsPtr interface{}, query string, args ...interface{}) error {
	return c.parseRows(rowsPtr, -1, query, args...)
}

func (c *Connection) Row(rowPtr interface{}, query string, args ...interface{}) error {
	if reflect.ValueOf(rowPtr).Type().Kind() != reflect.Ptr {
		return Errorf(
			"rowPtr must be a pointer, but %s.",
			reflect.ValueOf(rowPtr).Type().Kind())
	}
	rowsPtr := reflect.New(reflect.SliceOf(reflect.ValueOf(rowPtr).Type().Elem()))
	if err := c.parseRows(rowsPtr.Interface(), 1, query, args...); err != nil {
		return err
	}
	if rowsPtr.Elem().Len() != 1 {
		return Errorf("# of results must be 1, but %d.", rowsPtr.Elem().Len())
	}
	reflect.ValueOf(rowPtr).Elem().Set(rowsPtr.Elem().Index(0))
	return nil
}

func (c *Connection) RowsOrDie(rows interface{}, query string, args ...interface{}) {
	err := c.Rows(rows, query, args...)
	if err != nil {
		panic(err)
	}
}

func (c *Connection) RowOrDie(rowPtr interface{}, query string, args ...interface{}) {
	err := c.Row(rowPtr, query, args...)
	if err != nil {
		panic(err)
	}
}
