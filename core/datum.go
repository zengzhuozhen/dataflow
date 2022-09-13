package core

import "time"

type Datum struct {
	Key              string
	Value            any
	EventTime        time.Time
	NeedCancelBefore bool // meaning this key before value should be drop
}
