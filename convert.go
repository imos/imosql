package imosql

import (
	"strconv"
	"time"
)

func convertStringToInteger(stringResult string) (result int64, err error) {
	result, err = strconv.ParseInt(stringResult, 10, 64)
	return
}

func convertStringToTime(stringResult string) (result time.Time, err error) {
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		return
	}
	result, err = time.ParseInLocation("2006-01-02 15:04:05", stringResult, loc)
	return
}
