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

type CounterTrigger struct {
	count            int
	lastTriggerCount int
	readyChan        chan struct{}
}

func (c CounterTrigger) OnReady() <-chan struct{} {
	return c.readyChan
}

func (c CounterTrigger) Clone() Trigger {
	return CounterTrigger{
		count:     c.count,
		readyChan: make(chan struct{}),
	}
}

func (c CounterTrigger) Run(ctx context.Context, windowBase *windowBase) {
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

func (c CounterTrigger) GetParams() int {
	return c.count
}

func NewCounterTrigger(count int) Trigger {
	return CounterTrigger{count: count, readyChan: make(chan struct{})}
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

func (t TimeTrigger) GetParams() time.Duration {
	return t.period
}

func NewTimerTrigger(period time.Duration) Trigger {
	return TimeTrigger{tick: time.NewTicker(period), period: period, readyChan: make(chan struct{})}
}
