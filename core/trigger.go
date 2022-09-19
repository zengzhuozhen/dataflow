package core

import (
	"context"
	"time"
)

const (
	TriggerTypeCounterTrigger = 1
	TriggerTypeTimerTrigger   = 2
)

type Trigger interface {
	OnReady() <-chan struct{}
	Clone() Trigger
	Run(ctx context.Context, windowBase *windowBase)
}

type counterTrigger struct {
	count            int
	lastTriggerCount int
	readyChan        chan struct{}
}

func (c counterTrigger) OnReady() <-chan struct{} {
	return c.readyChan
}

func (c counterTrigger) Clone() Trigger {
	return counterTrigger{
		count:     c.count,
		readyChan: make(chan struct{}),
	}
}

func (c counterTrigger) Run(ctx context.Context, windowBase *windowBase) {
	for {
		select {
		case <-ctx.Done():
			close(c.readyChan)
			return
		default:
			if len(windowBase.data) >= c.count && len(windowBase.data) != c.lastTriggerCount {
				c.lastTriggerCount = len(windowBase.data)
				c.readyChan <- struct{}{}
			}
		}
	}
}

func NewCounterTrigger(count int) Trigger {
	return counterTrigger{count: count, readyChan: make(chan struct{})}
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
