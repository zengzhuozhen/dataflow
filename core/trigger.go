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
	OnReady() <-chan string
	Clone() Trigger
	Run(ctx context.Context, windowBase *windowBase)
}

type CounterTrigger struct {
	count               int
	readyChan           chan string
	lastTriggerCountMap map[string]int
}

func (c CounterTrigger) OnReady() <-chan string {
	return c.readyChan
}

func (c CounterTrigger) Clone() Trigger {
	return CounterTrigger{
		count:     c.count,
		readyChan: make(chan string),
	}
}

func (c CounterTrigger) Run(ctx context.Context, windowBase *windowBase) {
	for {
		select {
		case <-ctx.Done():
			close(c.readyChan)
			return
		default:
			for key, data := range windowBase.GroupByKey(windowBase.data) {
				if len(data) >= c.count && len(data) != c.lastTriggerCountMap[key] {
					c.lastTriggerCountMap[key] = len(data)
					c.readyChan <- key
				}
			}
		}
	}
}

func (c CounterTrigger) GetParams() int {
	return c.count
}

func NewCounterTrigger(count int) Trigger {
	return CounterTrigger{count: count, readyChan: make(chan string), lastTriggerCountMap: make(map[string]int)}
}

type TimeTrigger struct {
	tick      *time.Ticker
	period    time.Duration
	readyChan chan string
}

func (t TimeTrigger) OnReady() <-chan string {
	return t.readyChan
}

func (t TimeTrigger) Clone() Trigger {
	return TimeTrigger{
		tick:      time.NewTicker(t.period),
		period:    t.period,
		readyChan: make(chan string),
	}
}

func (t TimeTrigger) Run(ctx context.Context, windowBase *windowBase) {
	for {
		select {
		case <-ctx.Done():
			close(t.readyChan)
			return
		case <-t.tick.C:
			t.readyChan <- ""
		}
	}
}

func (t TimeTrigger) GetParams() time.Duration {
	return t.period
}

func NewTimerTrigger(period time.Duration) Trigger {
	return TimeTrigger{tick: time.NewTicker(period), period: period, readyChan: make(chan string)}
}
