package async

import (
	"fmt"
	"runtime"
	"slices"
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
	// waiters are the goroutines and the clock waits that wait on a waitable,
	// by the waitable's handle. The host numbers the handles of a component
	// instance from 1 and reuses freed numbers, so the slice is only as long as
	// the most handles alive at once. A slice, not a map: a map draws from the
	// program's random state when it is created, iterated or emptied.
	waiters []waiter
	// pending counts the goroutines among waiters.
	pending  int
	yielding chan unit
	pinner   runtime.Pinner
	// waits are the host clock waits armed so that the host resumes the task
	// when a Go timer is due, in the order of their domains in the runtime's
	// domain list, so that armTimer walks them together with the domains. A
	// wait that has completed stays until the next walk drops it.
	waits []*clockWait
	// spare is the slice the next walk builds waits in.
	spare []*clockWait
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
				w := state.take(event1)
				if w.wait != nil {
					// The Go timers it stood for are due; the scheduler runs
					// them before waking this goroutine again.
					w.wait.subtask = 0
					wasiAdvanceTime(w.wait.dom, w.wait.due)
					break
				}
				w.channel <- event2

			default:
				panic("todo")
			}

		case EVENT_STREAM_READ, EVENT_STREAM_WRITE, EVENT_FUTURE_READ, EVENT_FUTURE_WRITE:
			waitableJoin(event1, 0)
			state.take(event1).channel <- event2

		default:
			panic("todo")
		}

		// Block this goroutine until the scheduler wakes us up.
		(<-state.channel)

		if state.yielding != nil {
			contextSet(unsafe.Pointer(state))
			if state.pending == 0 {
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
			if state.pending == 0 && timer == timerNone {
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

// A waiter waits on one waitable: a goroutine, through its channel, or a clock
// wait.
type waiter struct {
	channel chan uint32
	wait    *clockWait
}

func (s *taskState) await(handle uint32, w waiter) {
	if n := int(handle) + 1; n > len(s.waiters) {
		s.waiters = slices.Grow(s.waiters, n-len(s.waiters))[:n]
	}
	s.waiters[handle] = w
	if w.channel != nil {
		s.pending++
	}
}

func (s *taskState) take(handle uint32) waiter {
	var w waiter
	if int(handle) < len(s.waiters) {
		w = s.waiters[handle]
		s.waiters[handle] = waiter{}
	}
	switch {
	case w.channel != nil:
		s.pending--
	case w.wait == nil:
		panic(fmt.Sprintf("nothing waits on waitable %v", handle))
	}
	return w
}

// clockWait is a host clock wait armed for the timers of one runtime domain.
type clockWait struct {
	// subtask is 0 once the wait has completed or been cancelled.
	subtask uint32
	due     int64
	dom     unsafe.Pointer
}

// A waitWalk carries the clock waits over to the next walk of the runtime's
// domains, dropping the waits that have completed. The runtime appends a new
// domain to its list and removes a closed one without reordering the rest, and
// a program cancels the waits of a domain before it closes it, so the waits of
// each domain come right after the waits of the domains before it.
type waitWalk struct {
	in  []*clockWait
	k   int
	out []*clockWait
}

func (s *taskState) walkWaits() waitWalk {
	return waitWalk{in: s.waits, out: s.spare[:0]}
}

// domain carries over the waits of dom, the next domain in the runtime's list,
// and reports whether one of them ends by due.
func (w *waitWalk) domain(dom unsafe.Pointer, due int64) (armed bool) {
	for ; w.k < len(w.in) && w.in[w.k].dom == dom; w.k++ {
		c := w.in[w.k]
		if c.subtask == 0 {
			continue
		}
		w.out = append(w.out, c)
		if c.due <= due {
			armed = true
		}
	}
	return armed
}

// add records a wait armed for the domain last passed to domain.
func (w *waitWalk) add(c *clockWait) {
	w.out = append(w.out, c)
}

func (s *taskState) endWalk(w waitWalk) {
	// Waits left over belong to domains closed without CancelClockWaits. They
	// stay until they complete, and until then each domain after them arms one
	// more wait than it needs.
	for _, c := range w.in[w.k:] {
		if c.subtask != 0 {
			w.out = append(w.out, c)
		}
	}
	clear(w.in)
	s.waits, s.spare = w.out, w.in[:0]
}

//go:linkname wasiIdleWait runtime.wasiIdleWait
func wasiIdleWait(i int) (dom, ctx unsafe.Pointer, due int64, ok bool)

//go:linkname wasiInstallContext runtime.wasiInstallContext
func wasiInstallContext(ctx unsafe.Pointer)

//go:linkname wasiGetContext runtime.wasiGetContext
func wasiGetContext() unsafe.Pointer

//go:linkname wasiAdvanceTime runtime.wasiAdvanceTime
func wasiAdvanceTime(dom unsafe.Pointer, t int64)

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
	walk := state.walkWaits()
	defer func() { state.endWalk(walk) }()
	for i := 0; ; i++ {
		dom, ctx, due, ok := wasiIdleWait(i)
		if !ok {
			break
		}
		armed := walk.domain(dom, due)
		if due == 0 {
			continue
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
			w := &clockWait{subtask, due, dom}
			state.await(subtask, waiter{wait: w})
			walk.add(w)
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
		if w.subtask != 0 {
			cancelWait(w.subtask)
		}
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
		switch {
		case w.subtask == 0:
		case w.dom == dom:
			state.take(w.subtask)
			cancelWait(w.subtask)
		default:
			kept = append(kept, w)
		}
	}
	clear(state.waits[len(kept):])
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
		state.await(subtask, waiter{channel: channel})
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
		state.await(uint32(handle), waiter{channel: channel})
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
