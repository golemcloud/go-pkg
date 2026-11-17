package async

// The benchmarks measure the task state's bookkeeping of waiters and clock
// waits, without host calls. The package builds only for wasip1 with Golem's Go
// toolchain, so build them with
//
//	GOOS=wasip1 GOARCH=wasm go test -c -ldflags=-checklinkname=0 ./wit/async
//
// and run the binary in a WASI runtime that also provides the import
// wasi_snapshot_preview1.adapter_monotonic_clock_set_paused.

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"unsafe"
)

var completionOrders = []string{"fifo", "reverse", "random"}

// handles returns the handles 1..n in the order the host completes them.
func handles(n int, order string) []uint32 {
	h := make([]uint32, n)
	for i := range h {
		h[i] = uint32(i + 1)
	}
	switch order {
	case "reverse":
		for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
			h[i], h[j] = h[j], h[i]
		}
	case "random":
		rand.New(rand.NewPCG(1, 2)).Shuffle(n, func(i, j int) { h[i], h[j] = h[j], h[i] })
	}
	return h
}

func domains(n int) []unsafe.Pointer {
	d := make([]unsafe.Pointer, n)
	for i := range d {
		d[i] = unsafe.Pointer(new([8]byte))
	}
	return d
}

func perOp(b *testing.B, n int, unit string) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/"+unit)
}

// BenchmarkWaiters registers n goroutines that wait on a waitable each, then
// completes them in an order.
func BenchmarkWaiters(b *testing.B) {
	ch := make(chan uint32)
	for _, n := range []int{10, 100, 1000} {
		for _, order := range completionOrders {
			done := handles(n, order)
			b.Run(fmt.Sprintf("n=%d/%s", n, order), func(b *testing.B) {
				s := &taskState{}
				for b.Loop() {
					for h := range n {
						s.await(uint32(h+1), waiter{channel: ch})
					}
					for _, h := range done {
						if s.take(h).channel != ch {
							b.Fatal("wrong waiter")
						}
					}
				}
				if s.pending != 0 {
					b.Fatal("goroutines left waiting")
				}
				perOp(b, n, "completion")
			})
		}
	}
}

// BenchmarkIdleWalk is the walk of one idle transition over n domains, each
// with a pending timer and a clock wait already armed for it.
func BenchmarkIdleWalk(b *testing.B) {
	for _, n := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("domains=%d", n), func(b *testing.B) {
			s := &taskState{}
			doms := domains(n)
			walk := s.walkWaits()
			for i, dom := range doms {
				walk.domain(dom, int64(1000+i))
				walk.add(&clockWait{uint32(i + 1), int64(1000 + i), dom})
			}
			s.endWalk(walk)
			for b.Loop() {
				walk := s.walkWaits()
				for i, dom := range doms {
					if !walk.domain(dom, int64(1000+i)) {
						b.Fatal("wait not found")
					}
				}
				s.endWalk(walk)
			}
			perOp(b, 1, "idle")
		})
	}
}

// BenchmarkClockWaitCompletion arms a clock wait in each of n domains, the host
// completes them in an order, and the next idle walk drops them.
func BenchmarkClockWaitCompletion(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		for _, order := range completionOrders {
			done := handles(n, order)
			b.Run(fmt.Sprintf("domains=%d/%s", n, order), func(b *testing.B) {
				s := &taskState{}
				doms := domains(n)
				for b.Loop() {
					walk := s.walkWaits()
					for i, dom := range doms {
						walk.domain(dom, int64(i+1))
						c := &clockWait{uint32(i + 1), int64(i + 1), dom}
						s.await(c.subtask, waiter{wait: c})
						walk.add(c)
					}
					s.endWalk(walk)
					for _, h := range done {
						s.take(h).wait.subtask = 0
					}
					walk = s.walkWaits()
					for _, dom := range doms {
						walk.domain(dom, 0)
					}
					s.endWalk(walk)
				}
				if len(s.waits) != 0 {
					b.Fatal("completed waits kept")
				}
				perOp(b, n, "completion")
			})
		}
	}
}
