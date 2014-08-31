package imosql

import (
	"time"
)

var lastAdjustTime int64 = 0
var timeGapInNanoseconds *int64 = nil

func (c *Connection) CurrentTime() time.Time {
	if timeGapInNanoseconds == nil || lastAdjustTime < time.Now().Unix()-600 {
		timeGapInNanoseconds = new(int64)
		*timeGapInNanoseconds =
			time.Now().UnixNano() - c.TimeOrDie("SELECT UTC_TIMESTAMP()").UnixNano()
		Logf("the current time gap is %d ms.", *timeGapInNanoseconds/1000000)
		lastAdjustTime = time.Now().Unix()
	}
	return time.Unix(0, time.Now().UnixNano()-*timeGapInNanoseconds)
}
