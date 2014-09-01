package imosql

import (
	"database/sql"
	"flag"
	"reflect"
	"time"
)

// Connection stores a SQL conneciton and provides main utility functions of
// ImoSQL.
type Connection struct {
	sql *sql.DB
}

var connection *Connection = nil
var driverName = flag.String(
	"driver_name", "mysql", "Specifies a driver name.")
var dataSourceName = flag.String(
	"data_source_name", "",
	"Specifies a driver-specific data source name. This flag overrides the "+
		"default data source name.")

// Open opens a database specified by its database driver name and a
// driver-specific data source name, which are the same arguments as
// the database/sql package uses.
func Open(defaultDriverName string, defaultDataSourceName string) (connection *Connection, err error) {
	connection = new(Connection)
	if *dataSourceName != "" {
		connection.sql, err = sql.Open(*driverName, *dataSourceName)
	} else {
		connection.sql, err = sql.Open(defaultDriverName, defaultDataSourceName)
	}
	if err != nil {
		err = errorf("failed to connect to the databse: %s", err)
		return
	}
	return
}

// Ping verifies a connection th the databse is still alive, estabilishing a
// connecion if necessary.  This function just calls DB.Ping in database/sql.
func (c *Connection) Ping() error {
	return c.sql.Ping()
}

////////////////////////////////////////////////////////////////////////////////
// No-value query functions
////////////////////////////////////////////////////////////////////////////////

// Execute runs a SQL command using DB.Exec.  This is primitive and returns
// sql.Result, which is returned by DB.Exec.  If sql.Result is not necessary,
// Connection.Change or Connection.Command should be used instead.  When ImoSQL
// logging is enabled, this function tries to output the last insert ID and the
// number of affected rows by the query.
func (c *Connection) Execute(query string, args ...interface{}) (result sql.Result, err error) {
	printLogf("running a SQL command: %s; %v.", query, args)
	result, err = c.sql.Exec(query, args...)
	if err != nil {
		err = errorf("failed to run a SQL command: %s", err)
		return
	}
	if IsLogging() {
		insertId, err := result.LastInsertId()
		if err == nil && insertId != 0 {
			printLogf("last insert ID is %d.", insertId)
		}
		rowsAffected, err := result.RowsAffected()
		if err == nil {
			printLogf("# of affected rows is %d.", rowsAffected)
		}
		err = nil
	}
	return
}

// ExecuteOrDie runs Connection.Execute.  If Connection.Execute fails,
// ExecuteOrDie panics.
func (c *Connection) ExecuteOrDie(query string, args ...interface{}) sql.Result {
	result, err := c.Execute(query, args...)
	if err != nil {
		panic(err)
	}
	return result
}

// Command runs a SQL command.
func (c *Connection) Command(query string, args ...interface{}) error {
	_, err := c.Execute(query, args...)
	return err
}

// CommandOrDie runs Connection.Command.  If Connection.Command fails, this
// function panics.
func (c *Connection) CommandOrDie(query string, args ...interface{}) {
	err := c.Command(query, args...)
	if err != nil {
		panic(err)
	}
}

// Change runs a SQL command changing a SQL table.  If the command changes
// nothing, Change returns an error.  Be careful that UPDATE, which is a SQL
// command, may change nothing even if it matches some rows if it results in
// changing nothing.
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
		return errorf("no row was updated.")
	}
	return nil
}

// ChangeOrDie runs Connection.Change.  If Connection.Change fails, this
// function panics.
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
	printLogf("running a SQL query: %s; %v.", query, args)
	rows, err := c.sql.Query(query, args...)
	if err != nil {
		return errorf("failed to run a SQL query: %s", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return errorf("no result.")
	}
	var stringResult string
	err = rows.Scan(&stringResult)
	if err != nil {
		return errorf("failed to scan one field: %s", err)
	}
	err = parseField(reflect.ValueOf(result), stringResult)
	if err != nil {
		return errorf("failed to parse a field: %s", err)
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
		return errorf("failed to create a RowReader: %s", err)
	}
	printLogf("running a SQL query: %s; %v.", query, args)
	inputRows, err := c.sql.Query(query, args...)
	if err != nil {
		return errorf("failed to run a SQL query: %s", err)
	}
	defer inputRows.Close()
	columns, err := inputRows.Columns()
	if err != nil {
		return errorf("failed to get columns: %s", err)
	}
	if len(columns) == 0 {
		return errorf("no columns.")
	}
	rowReader.SetColumns(columns)
	if err := rowReader.Read(inputRows, limit); err != nil {
		return errorf("failed to read rows: %s", err)
	}
	return nil
}

func (c *Connection) Rows(rowsPtr interface{}, query string, args ...interface{}) error {
	return c.parseRows(rowsPtr, -1, query, args...)
}

// Row fills rowPtr with a result for a given SQL query.  This function returns
// true iff there is at least one results, otherwise returns false.
func (c *Connection) Row(rowPtr interface{}, query string, args ...interface{}) (found bool, err error) {
	if reflect.ValueOf(rowPtr).Type().Kind() != reflect.Ptr {
		err = errorf(
			"rowPtr must be a pointer, but %s.",
			reflect.ValueOf(rowPtr).Type().Kind())
		return
	}
	rowsPtr := reflect.New(reflect.SliceOf(reflect.ValueOf(rowPtr).Type().Elem()))
	err = c.parseRows(rowsPtr.Interface(), 1, query, args...)
	if err != nil {
		return
	}
	if rowsPtr.Elem().Len() == 1 {
		reflect.ValueOf(rowPtr).Elem().Set(rowsPtr.Elem().Index(0))
		found = true
	}
	return
}

func (c *Connection) RowsOrDie(rowsPtr interface{}, query string, args ...interface{}) {
	err := c.Rows(rowsPtr, query, args...)
	if err != nil {
		panic(err)
	}
}

// RowOrDie runs Connection.Row. If Connection.Row fails, this function panics.
// This function returns true iff there is at least one results, otherwise
// returns false.
func (c *Connection) RowOrDie(rowPtr interface{}, query string, args ...interface{}) bool {
	found, err := c.Row(rowPtr, query, args...)
	if err != nil {
		panic(err)
	}
	return found
}
