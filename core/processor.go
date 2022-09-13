package core

import (
	"context"
)

type Processor struct {
	ctx      context.Context
	windows  Windows
	trigger  Trigger
	operator Operator
	evictor  Evictor
	input    chan Datum
	output   chan Datum
}

func BuildProcessor() *Processor {
	return &Processor{ctx: context.Background()}
}

func (p *Processor) Start() {
	go func() {
		for {
			select {
			case data := <-p.input:
				windows := p.windows.AssignWindow(data)
				if len(windows) == 0 {
					// can't found suitable window, create window and re-assign
					windows = p.windows.CreateWindow(data, p.trigger, p.operator, p.evictor)
					for _, window := range windows {
						window.start(p.output)
					}
					p.windows.AssignWindow(data)
				}
			}
		}
	}()
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

func (p *Processor) Build() (*Processor, chan<- Datum, <-chan Datum) {
	if p.windows == nil {
		panic("window must be set")
	}
	if p.trigger == nil {
		panic("trigger must be set")
	}
	if p.operator == nil {
		panic("operator must be set")
	}
	p.input = make(chan Datum)
	p.output = make(chan Datum)
	return p, p.input, p.output
}
