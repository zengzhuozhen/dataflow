package core

import (
	"time"
)

const (
	TriggerTypeCountTrigger = 1
	TriggerTypeTimeTrigger  = 2
)

type Trigger interface {
	OnReady() <-chan struct{}
	Clone() Trigger
	Run(windowBase *windowBase)
}

type countTrigger struct {
	count      int
	notifyChan chan struct{}
	readyChan  chan struct{}
}

func (c countTrigger) OnReady() <-chan struct{} {
	return c.readyChan
}

func (c countTrigger) Clone() Trigger {
	return countTrigger{
		count:      c.count,
		notifyChan: make(chan struct{}),
		readyChan:  make(chan struct{}),
	}
}

func (c countTrigger) Run(windowBase *windowBase) {
	for {
		select {
		// todo cancel
		default:
			if len(windowBase.data) >= c.count {
				c.readyChan <- struct{}{}
			}
		}
	}
}

func NewCountTrigger(count int) Trigger {
	return countTrigger{count: count, notifyChan: make(chan struct{})}
}

type TimeTrigger struct {
	tick      *time.Ticker
	period    time.Duration
	readyChan chan struct{}
}

func (t TimeTrigger) OnReady() <-chan struct{} {
	return t.readyChan
}

func (t TimeTrigger) Clone() Trigger {
	return TimeTrigger{
		tick:      time.NewTicker(t.period),
		period:    t.period,
		readyChan: make(chan struct{}),
	}
}

func (t TimeTrigger) Run(windowBase *windowBase) {
	for {
		select {
		// todo cancel
		case <-t.tick.C:
			t.readyChan <- struct{}{}
		}
	}
}

func NewTimerTrigger(period time.Duration) Trigger {
	return TimeTrigger{tick: time.NewTicker(period), period: period, readyChan: make(chan struct{})}
}
