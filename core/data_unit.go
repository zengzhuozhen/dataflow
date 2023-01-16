package core

import "time"

// DU dataUnit
type DU struct {
	Key              string
	Value            any
	EventTime        time.Time
	NeedCancelBefore bool // meaning this key before value should be dropped
}

type CalResultHandle func(du <-chan DU)
