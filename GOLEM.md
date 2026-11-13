# Golem's fork of go.bytecodealliance.org/pkg

This is [bytecodealliance/go-pkg](https://github.com/bytecodealliance/go-pkg)
with two patches that [Golem](https://github.com/golemcloud/golem)'s Go SDK needs.
Its `go.mod` still declares `module go.bytecodealliance.org/pkg`; Go components
built by the Golem CLI use it through

```
replace go.bytecodealliance.org/pkg => github.com/golemcloud/go-pkg <tag>
```

which the CLI adds to each component's `go.mod`.

## Patch 1: resume a task when a Go timer is due (`wit/async`)

The async task callback hands control to the host whenever every goroutine is
blocked, waiting on the task's subtasks, streams and futures. A Go timer is none
of these, so a task whose goroutines wait only on a timer — `time.Sleep`,
`time.After`, a context deadline — exited without producing its result, or stayed
suspended until an unrelated subtask completed.

When the task goes idle with a timer pending, the callback starts a
`wasi:clocks/monotonic-clock@0.3.0` `wait-until` the timer's due time and waits on
it with everything else. A timer keeps the task alive only until its closure has
returned, and clock waits still pending when the task exits are cancelled.

The runtime keeps a clock and a timers heap per runtime domain, so the callback
arms one wait per domain that has a pending timer, reading each domain's due time
and host context from `runtime.wasiIdleWait`. It installs that host context
around the wait, so the host attributes the wait to the part of the program
whose timer it is, and reports a completed wait with `runtime.wasiAdvanceTime`,
which advances that domain's clock. No clock is read here. `CancelClockWaits`
ends a domain's waits when the program closes the domain.

These functions exist only in
[Golem's Go toolchain fork](https://github.com/golemcloud/go) (patches 4 and 6
there), and the callback imports `wasi:clocks/monotonic-clock@0.3.0`, so the
component's world must import that interface.

## Patch 2: no map in the task state (`wit/async`)

The task state keeps the goroutines waiting on a waitable and the armed clock
waits in slices, not maps. Iterating a Go map, and deleting its last entry,
draw from the program's random state. The task state is shared by every part of
the program, so a map there would let a part of the program that a durable host
skips on replay shift the random stream of the rest.

## Branches and tags

`golem-v0.2` carries the patch on top of the upstream release it is based on.
Tags are `v<upstream>-golem.<n>`, e.g. `v0.2.3-golem.1`.
