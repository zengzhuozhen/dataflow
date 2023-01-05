package core

import "time"

// DU dateUnit
type DU struct {
	Key              string
	Value            any
	EventTime        time.Time
	NeedCancelBefore bool // meaning this key before value should be drop
}
