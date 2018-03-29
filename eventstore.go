package eventstore

type Event struct {
	Id      string
	Type    string
	Payload string
}

type EventInbound = chan<- *Event
type EventOutbound = <-chan *Event

type EventStore interface {
	Start() error
	Stop()
	Replay(string) EventOutbound
	Add() EventInbound
	Added() EventOutbound
}
