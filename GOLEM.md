# Golem's fork of go.bytecodealliance.org/pkg

This is [bytecodealliance/go-pkg](https://github.com/bytecodealliance/go-pkg)
with one patch that [Golem](https://github.com/golemcloud/golem)'s Go SDK needs.
Its `go.mod` still declares `module go.bytecodealliance.org/pkg`; Go components
built by the Golem CLI use it through

```
replace go.bytecodealliance.org/pkg => github.com/golemcloud/go-pkg <tag>
```

which the CLI adds to each component's `go.mod`.

## The patch: resume a task when a Go timer is due (`wit/async`)

The async task callback hands control to the host whenever every goroutine is
blocked, waiting on the task's subtasks, streams and futures. A Go timer is none
of these, so a task whose goroutines wait only on a timer — `time.Sleep`,
`time.After`, a context deadline — exited without producing its result, or stayed
suspended until an unrelated subtask completed.

When the task goes idle with a timer pending, the callback starts a
`wasi:clocks/monotonic-clock@0.3.0` `wait-for` until the timer is due and waits on
it with everything else. A timer keeps the task alive only until its closure has
returned, and clock waits still pending when the task exits are cancelled.

It reads the timer's due time from `runtime.wasiIdleTimer`, which only
[Golem's Go toolchain fork](https://github.com/golemcloud/go) provides (patch 4
there), and it imports `wasi:clocks/monotonic-clock@0.3.0`, so the component's
world must import that interface.

## Branches and tags

`golem-v0.2` carries the patch on top of the upstream release it is based on.
Tags are `v<upstream>-golem.<n>`, e.g. `v0.2.3-golem.1`.
