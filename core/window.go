package core

import (
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

var defaultGlobalWindow = GlobalWindow{
	windowBase: &windowBase{
		data:      []Datum{},
		startTime: time.Time{},
		endTime:   time.Time{},
		mutex:     new(sync.Mutex),
	},
	Once: new(sync.Once),
}

func NewDefaultGlobalWindow() Windows {
	return &defaultGlobalWindow
}

type Windows interface {
	// AssignWindow determine which window the coming data will drop in and return the window
	AssignWindow(data Datum) []*windowBase
	// CreateWindow create a list empty window for saving data,
	CreateWindow(data Datum, trigger Trigger, operator Operator, evictor Evictor) []*windowBase
}

type windowBase struct {
	data        []Datum
	startTime   time.Time
	endTime     time.Time
	mutex       *sync.Mutex
	trigger     Trigger
	operator    Operator
	evictor     Evictor
	closeNotify chan struct{}
}

func (wb *windowBase) start(output chan Datum) {
	go wb.trigger.Run(wb)
	go func() {
		for {
			select {
			case <-wb.closeNotify:
				return
			case <-wb.trigger.OnReady():
				wg := sync.WaitGroup{}
				for _, i := range wb.GroupByKey(wb.data) {
					wg.Add(1)
					go func(data []Datum) {
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

func (wb *windowBase) GroupByKey(dataList []Datum) map[string][]Datum {
	keyMap := make(map[string][]Datum)
	for _, data := range dataList {
		if i, exists := keyMap[data.Key]; exists {
			keyMap[data.Key] = append(i, data)
		} else {
			keyMap[data.Key] = []Datum{data}
		}
	}
	return keyMap
}

func (wb *windowBase) appendData(data Datum) {
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
	return baseTime.Add(passPeriod), baseTime.Add(passPeriod).Add(size)
}

type GlobalWindow struct {
	*windowBase
	*sync.Once
}

func (gw *GlobalWindow) AssignWindow(data Datum) []*windowBase {
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

func (gw *GlobalWindow) CreateWindow(data Datum, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	gw.windowBase = &windowBase{
		data:      []Datum{},
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

func (fw *FixedWindow) AssignWindow(data Datum) []*windowBase {
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

func (fw *FixedWindow) CreateWindow(data Datum, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	window := &windowBase{
		data:        []Datum{},
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

func NewSlideWindow(size, period time.Duration) *SlideWindow {
	return &SlideWindow{size: size, period: period}
}

func (sw *SlideWindow) AssignWindow(data Datum) []*windowBase {
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

func (sw *SlideWindow) CreateWindow(data Datum, trigger Trigger, operator Operator, evictor Evictor) (createdWindows []*windowBase) {
	var needPassPeriod time.Duration
	for {
		window := &windowBase{
			data:        []Datum{},
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

type SessionWindow struct {
	windows []*windowBase
	gap     time.Duration
}

func NewSessionWindow(gap time.Duration) Windows {
	return &SessionWindow{gap: gap}
}

func (sw *SessionWindow) AssignWindow(data Datum) []*windowBase {
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

func (sw *SessionWindow) CreateWindow(data Datum, trigger Trigger, operator Operator, evictor Evictor) []*windowBase {
	window := &windowBase{
		data:        []Datum{},
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
