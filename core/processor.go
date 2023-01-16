package core

import (
	"context"
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
				close(p.input)
				close(p.output)
				return
			case data := <-p.input:
				windows := p.windows.AssignWindow(data)
				if len(windows) == 0 {
					// can't found suitable window, create window and re-assign
					windows = p.windows.CreateWindow(data, p.trigger, p.operator, p.evictor)
					for _, window := range windows {
						window.start(p.ctx, p.output)
					}
					p.windows.AssignWindow(data)
				}
			}
		}
	}()
}

func (p *Processor) Stop() {
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

func (p *Processor) PushData(Data DU) {
	p.input <- Data
}

func (p *Processor) PopResult(fn CalResultHandle) {
	fn(p.output)
}
