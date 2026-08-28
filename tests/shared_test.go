// Package tests holds cross-package behavior tests that exercise the
// transport-neutral runtime — the task executor, orchestration replay, and
// entity batch execution — without a live task hub service.
//
// Every test here is deterministic: histories are hand-built rather than
// fetched from a service. End-to-end coverage against the only supported
// runtime lives in tests/durabletaskscheduler.
package tests

import "context"

var ctx = context.Background()
