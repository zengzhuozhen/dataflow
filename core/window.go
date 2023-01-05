package core

import (
	"context"
	"sort"
	"sync"
	"time"
)

type WindowType int32

const (
	WindowTypeGlobal WindowType = iota
	WindowTypeFixedWindow
	WindowTypeSlideWindow
	WindowTypeSessionWindow
)

type Windows interface {
	// AssignWindow determine which window the coming data will drop in and return the window
	AssignWindow(data DU) []*windowBase
	// CreateWindow create a list empty window for saving data,
	CreateWindow(data DU, trigger Trigger, operator Operator, evictor Evictor) []*windowBase
	// GetWindows return windows in processor
	GetWindows() []*windowBase
}

type windowBase struct {
	data        []DU
	startTime   time.Time
	endTime     time.Time
	mutex       *sync.Mutex
	trigger     Trigger
	operator    Operator
	evictor     Evictor
	closeNotify chan struct{}
}

func (wb *windowBase) start(ctx context.Context, output chan DU) {
	childCtx, cancel := context.WithCancel(ctx)
	go wb.trigger.Run(childCtx, wb)
	go func() {
		for {
			select {
			case <-ctx.Done():
				cancel()
				return
			case <-wb.closeNotify:
				cancel()
				return
			case <-wb.trigger.OnReady():
				wg := sync.WaitGroup{}
				for _, i := range wb.GroupByKey(wb.data) {
					wg.Add(1)
					go func(data []DU) {
						defer wg.Done()
						if wb.evictor != nil {
							wb.evictor.BeforeOperator(wb)
							defer wb.evictor.AfterOperator(wb)
						}
						output <- wb.operator.Operate(data)
					}(i)
				}
				wg.Wait()
			}
		}
	}()
}

func (wb *windowBase) stop() {
	wb.closeNotify <- struct{}{}
}

func (wb *windowBase) GroupByKey(dataList []DU) map[string][]DU {
	keyMap := make(map[string][]DU)
	for _, data := range dataList {
		if i, exists := keyMap[data.Key]; exists {
			keyMap[data.Key] = append(i, data)
		} else {
			keyMap[data.Key] = []DU{data}
		}
	}
	return keyMap
}

func (wb *windowBase) appendData(data DU) {
	wb.mutex.Lock()
	wb.data = append(wb.data, data)
	wb.mutex.Unlock()
}

func findStartAndEndTime(eventTime time.Time, size, passPeriod time.Duration) (start, end time.Time) {
	var baseTime time.Time
	if size >= time.Hour {
		baseTime = eventTime.Truncate(time.Duration(size.Hours()) * time.Hour)
	}
	if size < time.Hour && size >= time.Minute {
		baseTime = eventTime.Truncate(time.Duration(size.Minutes()) * time.Minute)
	}
	if size < time.Minute && size >= time.Second {
		baseTime = eventTime.Truncate(time.Duration(size.Seconds()) * time.Second)
	}
	return baseTime.Add(passPeriod), baseTime.Add(passPeriod).Add(size)
}

var defaultGlobalWindow = GlobalWindow{
	windowBase: &windowBase{
		data:      []DU{},
		startTime: time.Time{},
		endTime:   time.Time{},
		mutex:     new(sync.Mutex),
	},
	Once: new(sync.Once),
}

func (gw *GlobalWindow) GetWindows() []*windowBase {
	return []*windowBase{gw.windowBase}
}

func NewDefaultGlobalWindow() Windows {
	return &defaultGlobalWindow
}

type GlobalWindow struct {
	*windowBase
	*sync.Once
}

func (gw *GlobalWindow) AssignWindow(data DU) []*windowBase {
	window := gw.windowBase
	gw.appendData(data)
	gw.Once.Do(func() {
		// first enter should re-create window with trigger,operator and evitor
		window = nil
	})
	if window == nil {
		return []*windowBase{}
	}
	return []*windowBase{window}
}

func (gw *GlobalWindow) CreateWindow(data DU, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	gw.windowBase = &windowBase{
		data:      []DU{},
		startTime: time.Time{},
		endTime:   time.Time{},
		mutex:     new(sync.Mutex),
		trigger:   trigger.Clone(),
		operator:  operator.Clone(),
		evictor:   evictor.Clone(),
	}
	return []*windowBase{gw.windowBase}
}

type FixedWindow struct {
	windows []*windowBase
	size    time.Duration
}

func (fw *FixedWindow) GetParams() time.Duration {
	return fw.size
}

func (fw *FixedWindow) GetWindows() []*windowBase {
	return fw.windows
}

func (fw *FixedWindow) AssignWindow(data DU) []*windowBase {
	for _, window := range fw.windows {
		if (data.EventTime.After(window.startTime) || data.EventTime.Equal(window.startTime)) &&
			data.EventTime.Before(window.endTime) {
			window.appendData(data)
			return []*windowBase{window}
		}
	}
	// can't found suitable window,need a new one
	return []*windowBase{}
}

func (fw *FixedWindow) CreateWindow(data DU, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	window := &windowBase{
		data:        []DU{},
		mutex:       new(sync.Mutex),
		trigger:     trigger.Clone(),
		operator:    operator.Clone(),
		evictor:     evictor.Clone(),
		closeNotify: make(chan struct{}),
	}
	window.startTime, window.endTime = findStartAndEndTime(data.EventTime, fw.size, 0)
	fw.windows = append(fw.windows, window)
	sort.Slice(fw.windows, func(i, j int) bool {
		return fw.windows[i].startTime.Before(fw.windows[j].startTime)
	})
	return []*windowBase{window}
}

func NewFixedWindows(size time.Duration) *FixedWindow {
	return &FixedWindow{size: size}
}

type SlideWindow struct {
	windows []*windowBase
	size    time.Duration
	period  time.Duration
}

func (sw *SlideWindow) GetParams() (time.Duration, time.Duration) {
	return sw.size, sw.period
}

func (sw *SlideWindow) GetWindows() []*windowBase {
	return sw.windows
}

func (sw *SlideWindow) AssignWindow(data DU) []*windowBase {
	var assignWindows []*windowBase
	for _, window := range sw.windows {
		if (data.EventTime.After(window.startTime) || data.EventTime.Equal(window.startTime)) &&
			data.EventTime.Before(window.endTime) {
			window.appendData(data)
			assignWindows = append(assignWindows, window)
		}
	}
	return assignWindows
}

func (sw *SlideWindow) CreateWindow(data DU, trigger Trigger, operator Operator, evictor Evictor) (createdWindows []*windowBase) {
	var needPassPeriod time.Duration
	for {
		window := &windowBase{
			data:        []DU{},
			mutex:       new(sync.Mutex),
			trigger:     trigger.Clone(),
			operator:    operator.Clone(),
			evictor:     evictor.Clone(),
			closeNotify: make(chan struct{}),
		}
		window.startTime, window.endTime = findStartAndEndTime(data.EventTime, sw.size, needPassPeriod)
		sw.windows = append(sw.windows, window)
		createdWindows = append(createdWindows, window)
		if window.startTime.Add(sw.period).After(data.EventTime) {
			sort.Slice(sw.windows, func(i, j int) bool {
				return sw.windows[i].startTime.Before(sw.windows[j].startTime)
			})
			return
		}
		needPassPeriod += sw.period
	}
}

func NewSlideWindow(size, period time.Duration) *SlideWindow {
	return &SlideWindow{size: size, period: period}
}

type SessionWindow struct {
	windows []*windowBase
	gap     time.Duration
}

func (sw *SessionWindow) GetParams() time.Duration {
	return sw.gap
}

func (sw *SessionWindow) GetWindows() []*windowBase {
	return sw.windows
}

func (sw *SessionWindow) AssignWindow(data DU) []*windowBase {
	var assignWindows []*windowBase

	for index, window := range sw.windows {
		if (data.EventTime.After(window.startTime) || data.EventTime.Equal(window.startTime)) &&
			data.EventTime.Before(window.endTime) {
			window.appendData(data) // maybe should exec after merge window
			// expansion window
			if window.startTime.After(data.EventTime.Add(-sw.gap / 2)) {
				window.startTime = data.EventTime.Add(-sw.gap / 2)
			}
			if window.endTime.Before(data.EventTime.Add(sw.gap / 2)) {
				window.endTime = data.EventTime.Add(sw.gap / 2)
			}
			window = sw.tryMerge(index, window)
			assignWindows = append(assignWindows, window)
		}
	}
	return assignWindows
}

func (sw *SessionWindow) tryMerge(index int, window *windowBase) *windowBase {
	hadMerged := true
	for {
		//  check can merge prev and later window
		if index > 0 && (index+1 < len(sw.windows)) && hadMerged { // not the first on and last one

			hadMerged = false
			if window.endTime.After(sw.windows[index+1].startTime) ||
				window.endTime.Equal(sw.windows[index+1].startTime) { // merge later
				window = sw.mergeWindow(index, index+1)
				hadMerged = true
			}
			if window.startTime.Before(sw.windows[index-1].endTime) ||
				window.startTime.Equal(sw.windows[index+1].endTime) { // merge prev
				window = sw.mergeWindow(index-1, index)
				hadMerged = true
			}
			continue
		}
		return window
	}
}

func (sw *SessionWindow) CreateWindow(data DU, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	window := &windowBase{
		data:        []DU{},
		mutex:       new(sync.Mutex),
		trigger:     trigger.Clone(),
		operator:    operator.Clone(),
		evictor:     evictor.Clone(),
		closeNotify: make(chan struct{}),
	}
	window.startTime, window.endTime = findStartAndEndTime(data.EventTime, sw.gap, 0)
	sw.windows = append(sw.windows, window)
	sort.Slice(sw.windows, func(i, j int) bool {
		return sw.windows[i].startTime.Before(sw.windows[j].startTime)
	})
	return []*windowBase{window}
}

func NewSessionWindow(gap time.Duration) Windows {
	return &SessionWindow{gap: gap}
}

func (sw *SessionWindow) mergeWindow(windowIndex1, windowIndex2 int) *windowBase {
	window1 := sw.windows[windowIndex1]
	window2 := sw.windows[windowIndex2]
	window2.stop()

	window1.data = append(window1.data, window2.data...)
	if window2.startTime.Before(window1.startTime) {
		window1.startTime = window2.startTime
	}
	if window2.endTime.After(window1.endTime) {
		window1.endTime = window2.endTime
	}
	sw.windows = append(sw.windows[0:windowIndex2], sw.windows[windowIndex2:]...)

	sort.Slice(sw.windows, func(i, j int) bool {
		return sw.windows[i].startTime.Before(sw.windows[j].startTime)
	})
	return window1
}
