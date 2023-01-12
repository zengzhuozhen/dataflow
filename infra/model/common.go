package model

type Resource interface {
	*CalTask | *Evictor | *Operator | *Trigger | *Window
}
