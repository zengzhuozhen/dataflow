package pkg

import (
	"context"
	"sync"
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
	Reset(key string)
}

type CounterTrigger struct {
	count               int
	readyChan           chan string
	lastTriggerCountMap sync.Map
}

func (c *CounterTrigger) OnReady() <-chan string {
	return c.readyChan
}

func (c *CounterTrigger) Clone() Trigger {
	return &CounterTrigger{
		count:     c.count,
		readyChan: make(chan string),
	}
}

func (c *CounterTrigger) Run(ctx context.Context, windowBase *windowBase) {
	defer close(c.readyChan)
	for {
		select {
		case <-ctx.Done():
			return
		case <-windowBase.notifyChan:
			for key, data := range windowBase.GroupByKey(windowBase.data) {
				val, _ := c.lastTriggerCountMap.LoadOrStore(key, 0)
				lastCount := val.(int)
				if len(data) >= c.count && len(data) != lastCount {
					c.lastTriggerCountMap.Store(key, len(data))
					c.readyChan <- key
				}
			}
		}
	}
}

func (c *CounterTrigger) Reset(key string) {
	c.lastTriggerCountMap.Store(key, 0)
}

func (c *CounterTrigger) GetParams() int {
	return c.count
}

func NewCounterTrigger(count int) Trigger {
	return &CounterTrigger{count: count, readyChan: make(chan string)}
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
	defer close(t.readyChan)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.tick.C:
			t.readyChan <- ""
		}
	}
}

func (t TimeTrigger) GetParams() time.Duration {
	return t.period
}

func (t TimeTrigger) Reset(string) {
	t.tick.Reset(t.period)
}

func NewTimerTrigger(period time.Duration) Trigger {
	return TimeTrigger{tick: time.NewTicker(period), period: period, readyChan: make(chan string)}
}
