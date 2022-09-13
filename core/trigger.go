package core

import (
	"context"
	"time"
)

const (
	TriggerTypeCountTrigger = 1
	TriggerTypeTimeTrigger  = 2
)

type Trigger interface {
	OnReady() <-chan struct{}
	Clone() Trigger
	Run(ctx context.Context, windowBase *windowBase)
}

type countTrigger struct {
	count     int
	readyChan chan struct{}
}

func (c countTrigger) OnReady() <-chan struct{} {
	return c.readyChan
}

func (c countTrigger) Clone() Trigger {
	return countTrigger{
		count:     c.count,
		readyChan: make(chan struct{}),
	}
}

func (c countTrigger) Run(ctx context.Context, windowBase *windowBase) {
	for {
		select {
		case <-ctx.Done():
			close(c.readyChan)
			return
		default:
			if len(windowBase.data) >= c.count {
				c.readyChan <- struct{}{}
			}
		}
	}
}

func NewCountTrigger(count int) Trigger {
	return countTrigger{count: count, readyChan: make(chan struct{})}
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

func (t TimeTrigger) Run(ctx context.Context, windowBase *windowBase) {
	for {
		select {
		case <-ctx.Done():
			close(t.readyChan)
			return
		case <-t.tick.C:
			t.readyChan <- struct{}{}
		}
	}
}

func NewTimerTrigger(period time.Duration) Trigger {
	return TimeTrigger{tick: time.NewTicker(period), period: period, readyChan: make(chan struct{})}
}
