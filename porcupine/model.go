package porcupine

import "fmt"

type Operation struct {
	ClientId int // optional, unless you want a visualization; zero-indexed
	Input    interface{}
	Call     int64 // invocation time
	Output   interface{}
	Return   int64 // response time
}

type EventKind bool

const (
	CallEvent   EventKind = false
	ReturnEvent EventKind = true
)

type Event struct {
	ClientId int // optional, unless you want a visualization; zero-indexed
	Kind     EventKind
	Value    interface{}
	Id       int
}

type Model struct {
	// Partition functions, such that a history is linearizable if and only
	// if each partition is linearizable. If you don't want to implement
	// this, you can always use the `NoPartition` functions implemented
	// below.
	Partition      func(history []Operation) [][]Operation
	PartitionEvent func(history []Event) [][]Event
	// Initial state of the system.
	Init func() interface{}
	// Step function for the system. Returns whether or not the system
	// could take this step with the given inputs and outputs and also
	// returns the new state. This should not mutate the existing state.
	Step func(state interface{}, input interface{}, output interface{}) (bool, interface{})
	// Equality on states. If you are using a simple data type for states,
	// you can use the `ShallowEqual` function implemented below.
	Equal func(state1, state2 interface{}) bool
	// For visualization, describe an operation as a string.
	// For example, "Get('x') -> 'y'".
	DescribeOperation func(input interface{}, output interface{}) string
	// For visualization purposes, describe a state as a string.
	// For example, "{'x' -> 'y', 'z' -> 'w'}"
	DescribeState func(state interface{}) string
}

func NoPartition(history []Operation) [][]Operation {
	return [][]Operation{history}
}

func NoPartitionEvent(history []Event) [][]Event {
	return [][]Event{history}
}

func ShallowEqual(state1, state2 interface{}) bool {
	return state1 == state2
}

func DefaultDescribeOperation(input interface{}, output interface{}) string {
	return fmt.Sprintf("%v -> %v", input, output)
}

func DefaultDescribeState(state interface{}) string {
	return fmt.Sprintf("%v", state)
}

type CheckResult string

const (
	Unknown CheckResult = "Unknown" // timed out
	Ok                  = "Ok"
	Illegal             = "Illegal"
)
