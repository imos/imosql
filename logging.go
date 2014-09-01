package imosql

import (
	"flag"
	"fmt"
	"log"
	"strings"
)

var isLogging = flag.Bool("imosql_logging", false, "Show SQL queries.")

func SetLogging(mode bool) {
	isLogging = new(bool)
	*isLogging = mode
}

func IsLogging() bool {
	return *isLogging
}

func printLog(a ...interface{}) {
	if IsLogging() {
		message := fmt.Sprint(a...)
		log.Println(strings.TrimSpace(message))
	}
	return
}

func printLogf(format string, a ...interface{}) {
	if IsLogging() {
		message := fmt.Sprintf(format, a...)
		log.Println(strings.TrimSpace(message))
	}
	return
}

func errorf(format string, a ...interface{}) (err error) {
	err = fmt.Errorf(format, a...)
	printLog(err)
	return
}
