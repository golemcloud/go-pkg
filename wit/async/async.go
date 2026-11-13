package async

import (
	"fmt"
	"runtime"
	"unsafe"

	witRuntime "go.bytecodealliance.org/pkg/wit/runtime"
)

// Note: These constants have specific values defined by the canonical ABI, so we use explicit values instead of iota.

const (
	EVENT_NONE         uint32 = 0
	EVENT_SUBTASK      uint32 = 1
	EVENT_STREAM_READ  uint32 = 2
	EVENT_STREAM_WRITE uint32 = 3
	EVENT_FUTURE_READ  uint32 = 4
	EVENT_FUTURE_WRITE uint32 = 5
)

const (
	STATUS_STARTING uint32 = 0
	STATUS_STARTED  uint32 = 1
	STATUS_RETURNED uint32 = 2
)

const (
	CALLBACK_CODE_EXIT  uint32 = 0
	CALLBACK_CODE_YIELD uint32 = 1
	CALLBACK_CODE_WAIT  uint32 = 2
)

const (
	RETURN_CODE_BLOCKED   uint32 = 0xFFFFFFFF
	RETURN_CODE_COMPLETED uint32 = 0
	RETURN_CODE_DROPPED   uint32 = 1
)

type unit struct{}

type taskState struct {
	channel     chan unit
	waitableSet uint32
	// pending are the goroutines waiting on a waitable. A slice, not a map: a
	// map that becomes empty is reseeded from the program's random state.
	pending  []pendingWait
	yielding chan unit
	pinner   runtime.Pinner
	// waits are the host clock waits armed so that the host resumes the task
	// when a Go timer is due. A slice, not a map: iterating a map draws from the
	// program's random state.
	waits []clockWait
	// done records that the task's closure has returned, after which a pending
	// Go timer no longer keeps the task alive.
	done bool
}

var state *taskState = nil

func Run(closure func()) uint32 {
	state = &taskState{
		channel: make(chan unit),
	}
	state.pinner.Pin(state)

	defer func() {
		state = nil
	}()

	task := state
	go func() {
		closure()
		task.done = true
	}()

	return callback(EVENT_NONE, 0, 0)
}

func Callback(event0, event1, event2 uint32) uint32 {
	state = (*taskState)(contextGet())
	contextSet(nil)

	return callback(event0, event1, event2)
}

//go:linkname wasiOnIdle runtime.wasiOnIdle
func wasiOnIdle(callback func() bool)

func callback(event0, event1, event2 uint32) uint32 {
	yielding := state.yielding
	if state.yielding != nil {
		state.yielding = nil
		yielding <- unit{}
	}

	// Tell the Go scheduler to write to `state.channel` only after all
	// goroutines have either blocked or exited.  This allows us to reliably
	// delay returning control to the host until there's truly nothing more
	// we can do in the guest.
	//
	// Note that this function is _not_ currently part of upstream Go; it
	// requires [this
	// patch](https://github.com/dicej/go/commit/40fc123d5bce6448fc4e4601fd33bad4250b36a5)
	wasiOnIdle(func() bool {
		state.channel <- unit{}
		return true
	})
	defer wasiOnIdle(func() bool {
		return false
	})

	for {
		switch event0 {
		case EVENT_NONE:

		case EVENT_SUBTASK:
			switch event2 {
			case STATUS_STARTING:
				panic(fmt.Sprintf("unexpected subtask status: %v", event2))

			case STATUS_STARTED:

			case STATUS_RETURNED:
				waitableJoin(event1, 0)
				subtaskDrop(event1)
				if i := waitIndex(event1); i >= 0 {
					// The Go timers it stood for are due; the scheduler runs
					// them before waking this goroutine again.
					w := state.waits[i]
					state.waits = append(state.waits[:i], state.waits[i+1:]...)
					wasiAdvanceTime(w.dom, w.due)
					break
				}
				takePending(event1) <- event2

			default:
				panic("todo")
			}

		case EVENT_STREAM_READ, EVENT_STREAM_WRITE, EVENT_FUTURE_READ, EVENT_FUTURE_WRITE:
			waitableJoin(event1, 0)
			takePending(event1) <- event2

		default:
			panic("todo")
		}

		// Block this goroutine until the scheduler wakes us up.
		(<-state.channel)

		if state.yielding != nil {
			contextSet(unsafe.Pointer(state))
			if len(state.pending) == 0 {
				return CALLBACK_CODE_YIELD
			} else {
				if state.waitableSet == 0 {
					panic("unreachable")
				}
				event0, event1, event2 = func() (uint32, uint32, uint32) {
					pinner := runtime.Pinner{}
					defer pinner.Unpin()
					buffer := witRuntime.Allocate(&pinner, 8, 4)
					event0 := waitableSetPoll(state.waitableSet, buffer)
					return event0,
						unsafe.Slice((*uint32)(buffer), 2)[0],
						unsafe.Slice((*uint32)(buffer), 2)[1]
				}()
				if event0 == EVENT_NONE {
					return CALLBACK_CODE_YIELD
				}
			}
		} else {
			timer := timerNone
			if !state.done {
				timer = armTimer()
			}
			if timer == timerDue {
				// The timer is already due: give the host a turn, then let
				// the scheduler run it.
				contextSet(unsafe.Pointer(state))
				return CALLBACK_CODE_YIELD
			}
			if len(state.pending) == 0 && timer == timerNone {
				cancelTimers()
				state.pinner.Unpin()
				if state.waitableSet != 0 {
					waitableSetDrop(state.waitableSet)
				}
				return CALLBACK_CODE_EXIT
			}
			if state.waitableSet == 0 {
				panic("unreachable")
			}
			contextSet(unsafe.Pointer(state))
			return CALLBACK_CODE_WAIT | (state.waitableSet << 4)
		}
	}
}

const (
	timerNone = iota
	timerArmed
	timerDue
)

type pendingWait struct {
	waitable uint32
	channel  chan uint32
}

func takePending(waitable uint32) chan uint32 {
	for i, p := range state.pending {
		if p.waitable == waitable {
			state.pending = append(state.pending[:i], state.pending[i+1:]...)
			return p.channel
		}
	}
	panic(fmt.Sprintf("no goroutine waits on waitable %v", waitable))
}

// clockWait is a host clock wait armed for the timers of one runtime domain.
type clockWait struct {
	subtask uint32
	due     int64
	dom     unsafe.Pointer
}

//go:linkname wasiIdleWait runtime.wasiIdleWait
func wasiIdleWait(i int) (dom, ctx unsafe.Pointer, due int64, ok bool)

//go:linkname wasiInstallContext runtime.wasiInstallContext
func wasiInstallContext(ctx unsafe.Pointer)

//go:linkname wasiGetContext runtime.wasiGetContext
func wasiGetContext() unsafe.Pointer

//go:linkname wasiAdvanceTime runtime.wasiAdvanceTime
func wasiAdvanceTime(dom unsafe.Pointer, t int64)

func waitIndex(subtask uint32) int {
	for i, w := range state.waits {
		if w.subtask == subtask {
			return i
		}
	}
	return -1
}

// armTimer makes sure the host resumes the task when the runtime's next timer
// is due, by waiting on a host clock wait that ends then. Go timers are not
// waitables, so without one a task whose goroutines wait on a timer would be
// left suspended, or would exit without producing its result.
//
// The runtime keeps a clock per domain, and a domain's timers fire against its
// own clock. Each domain with a pending timer gets its own wait, armed in the
// domain's host context, until the due time of its next timer, so no clock is
// read here and the wait belongs to the part of the program whose timer it is.
func armTimer() int {
	result := timerNone
	own := wasiGetContext()
	for i := 0; ; i++ {
		dom, ctx, due, ok := wasiIdleWait(i)
		if !ok {
			break
		}
		if due == 0 {
			continue
		}
		armed := false
		for _, w := range state.waits {
			if w.dom == dom && w.due <= due {
				armed = true
				break
			}
		}
		if armed {
			if result == timerNone {
				result = timerArmed
			}
			continue
		}
		wasiInstallContext(ctx)
		status := uint32(monotonicWaitUntil(due))
		wasiInstallContext(own)
		subtask := status >> 4
		switch status & 0xF {
		case STATUS_STARTING, STATUS_STARTED:
			if state.waitableSet == 0 {
				state.waitableSet = waitableSetNew()
			}
			waitableJoin(subtask, state.waitableSet)
			state.waits = append(state.waits, clockWait{subtask, due, dom})
			if result == timerNone {
				result = timerArmed
			}
		case STATUS_RETURNED:
			wasiAdvanceTime(dom, due)
			result = timerDue
		default:
			panic(fmt.Sprintf("unexpected subtask status: %v", status&0xF))
		}
	}
	return result
}

// cancelTimers ends the clock waits still pending when the task exits.
func cancelTimers() {
	for _, w := range state.waits {
		cancelWait(w.subtask)
	}
	state.waits = nil
}

func cancelWait(subtask uint32) {
	waitableJoin(subtask, 0)
	subtaskCancel(subtask)
	subtaskDrop(subtask)
}

// CancelClockWaits ends the clock waits armed for the runtime domain dom. A
// program calls it when it closes a domain whose timers it has stopped, while
// the host context the waits were armed in is still valid.
func CancelClockWaits(dom unsafe.Pointer) {
	if state == nil {
		return
	}
	kept := state.waits[:0]
	for _, w := range state.waits {
		if w.dom == dom {
			cancelWait(w.subtask)
		} else {
			kept = append(kept, w)
		}
	}
	state.waits = kept
}

func SubtaskWait(status uint32) {
	subtask := status >> 4
	status = status & 0xF

	switch status {
	case STATUS_STARTING, STATUS_STARTED:
		if state.waitableSet == 0 {
			state.waitableSet = waitableSetNew()
		}
		waitableJoin(subtask, state.waitableSet)
		channel := make(chan uint32)
		state.pending = append(state.pending, pendingWait{subtask, channel})
		(<-channel)

	case STATUS_RETURNED:

	default:
		panic(fmt.Sprintf("unexpected subtask status: %v", status))
	}
}

func FutureOrStreamWait(code uint32, handle int32) (uint32, uint32) {
	if code == RETURN_CODE_BLOCKED {
		if state.waitableSet == 0 {
			state.waitableSet = waitableSetNew()
		}
		waitableJoin(uint32(handle), state.waitableSet)
		channel := make(chan uint32)
		state.pending = append(state.pending, pendingWait{uint32(handle), channel})
		code = (<-channel)
	}

	count := code >> 4
	code = code & 0xF

	return code, count
}

func Yield() {
	channel := make(chan unit)
	state.yielding = channel
	(<-channel)
}

//go:wasmimport $root [waitable-set-new]
//go:noescape
func waitableSetNew() uint32

//go:wasmimport $root [waitable-set-poll]
//go:noescape
func waitableSetPoll(waitableSet uint32, eventPayload unsafe.Pointer) uint32

//go:wasmimport $root [waitable-set-drop]
//go:noescape
func waitableSetDrop(waitableSet uint32)

//go:wasmimport $root [waitable-join]
//go:noescape
func waitableJoin(waitable, waitableSet uint32)

//go:wasmimport $root [context-get-0]
//go:noescape
func contextGet() unsafe.Pointer

//go:wasmimport $root [context-set-0]
//go:noescape
func contextSet(value unsafe.Pointer)

//go:wasmimport $root [subtask-drop]
//go:noescape
func subtaskDrop(subtask uint32)

//go:wasmimport $root [subtask-cancel]
//go:noescape
func subtaskCancel(subtask uint32) uint32

//go:wasmimport wasi:clocks/monotonic-clock@0.3.0 [async-lower]wait-until
//go:noescape
func monotonicWaitUntil(when int64) int32
