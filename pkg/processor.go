package pkg

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

type Processor struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ID       string
	windows  Windows
	trigger  Trigger
	operator Operator
	evictor  Evictor
	input    chan DU
	output   chan DU
	wg       sync.WaitGroup
}

func BuildProcessor() *Processor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Processor{ctx: ctx, cancel: cancel, ID: uuid.NewString()}
}

func (p *Processor) Start() {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				p.wg.Wait()
				close(p.output)
				return
			case data := <-p.input:
				windows := p.windows.AssignWindow(data)
				if len(windows) == 0 {
					// can't found suitable window, create window first
					windows = p.windows.CreateWindow(data, p.trigger, p.operator, p.evictor)
					for _, window := range windows {
						p.wg.Add(1)
						window.startWithWG(p.ctx, p.output, &p.wg)
					}
					// now trigger is running, safe to assign data
					p.windows.AssignWindow(data)
				}
			}
		}
	}()
}

func (p *Processor) Stop() {
	// 主动 stop 所有窗口，确保 goroutine 能收到退出信号
	if p.windows != nil {
		for _, w := range p.windows.GetWindows() {
			w.stop()
		}
	}
	p.cancel()
}

func (p *Processor) Window(windows Windows) *Processor {
	p.windows = windows
	return p
}

func (p *Processor) Trigger(trigger Trigger) *Processor {
	p.trigger = trigger
	return p
}

func (p *Processor) Operator(operator Operator) *Processor {
	p.operator = operator
	return p
}

func (p *Processor) Evictor(evictor Evictor) *Processor {
	p.evictor = evictor
	return p
}

func (p *Processor) Build() (*Processor, chan<- DU, <-chan DU) {
	if p.windows == nil {
		panic("window must be set")
	}
	if p.trigger == nil {
		panic("trigger must be set")
	}
	if p.operator == nil {
		panic("operator must be set")
	}
	p.input = make(chan DU)
	p.output = make(chan DU)
	return p, p.input, p.output
}

// Node接口实现
func (p *Processor) In() chan<- DU {
	return p.input
}

func (p *Processor) Out() <-chan DU {
	return p.output
}
