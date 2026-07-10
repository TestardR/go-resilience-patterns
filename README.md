# go-resilience-patterns

> Don't let overload or slowness in one part of the system bring everything else down.

Eight self-contained Go packages, each demonstrating one resilience pattern. No frameworks, no external dependencies — stdlib only. Read the code, understand the tradeoffs.

---

## Two directions

### Direction 1: Control how much work you accept

If your server accepts more requests than it can handle, everything slows down for everyone. That's why systems use rate limiting, bounded queues, or load shedding — they let the server say no early instead of letting latency spiral out of control. In some cases, you might still serve partial or cached results for less-critical work — that's called graceful degradation.

### Direction 2: Protect yourself from slow dependencies

Even if your code is fast, you might be waiting on something else — a database, another service, an API. You can use timeouts, retries with backoff, circuit breakers, and bulkheads to prevent one slow or failing component from blocking everything else. These techniques help your service fail gracefully rather than crash or hang.

---

## Pattern index

| Pattern | Group | Package | Description |
|---|---|---|---|
| Rate limiting | accept | [accept/ratelimit](accept/ratelimit) | Token bucket — cap requests per unit time; return 429 when exhausted |
| Bounded queue | accept | [accept/boundedqueue](accept/boundedqueue) | Fixed-capacity channel buffer — reject (503) when backlog is full |
| Load shedding | accept | [accept/loadshedding](accept/loadshedding) | In-flight counter — reject (503) when concurrent load exceeds threshold |
| Graceful degradation | accept | [accept/degradation](accept/degradation) | Serve cached/fallback response (200) when primary dependency fails |
| Timeout | protect | [protect/timeout](protect/timeout) | `context.WithTimeout` — abandon slow dependency calls; return 504 |
| Retry with backoff | protect | [protect/retry](protect/retry) | Bounded retries with exponential backoff + full jitter; idempotent ops only |
| Circuit breaker | protect | [protect/circuitbreaker](protect/circuitbreaker) | Closed/Open/Half-Open state machine — stop calling a failing dependency |
| Bulkhead | protect | [protect/bulkhead](protect/bulkhead) | Per-partition channel semaphores — isolate resource pools by tenant/route |

---

## How the patterns differ

**Three ways to control accepted work:**

- **Rate limiting** is temporal. A token bucket refills at a fixed rate, so you cap throughput over time. Bursts are absorbed up to bucket capacity, then you return 429.
- **Bounded queue** is spatial. A fixed-capacity channel holds pending work. When the backlog is full, new arrivals are rejected with 503 — no waiting, no queueing beyond the limit.
- **Bulkhead** is about isolation, not volume. It partitions concurrency by tenant, route, or any key, so one noisy caller can't exhaust the pool for everyone else.

**Two ways to handle a failing dependency on the accept side:**

- **Load shedding** counts in-flight requests. When concurrent load exceeds the threshold, it rejects with 503. The client knows to back off.
- **Graceful degradation** doesn't reject. When the primary dependency fails, it serves a cached or fallback response with 200. The client gets something useful, even if stale.

**Three ways to protect yourself from slow dependencies:**

- **Timeout** sets a `context.WithTimeout` on each outbound call. If the dependency doesn't respond in time, you cancel and return 504. This is a per-call deadline, not a server write timeout.
- **Retry with backoff** retries transient failures a bounded number of times, with exponential backoff and full jitter to avoid thundering herds. Only safe for idempotent operations.
- **Circuit breaker** tracks failure rate over a window. When failures exceed the threshold, it opens the circuit and stops calling the dependency entirely. After a cooldown, it enters half-open state and probes with a single request before closing again.

---

## Repo layout

```
go-resilience-patterns/
├── accept/
│   ├── ratelimit/        # token bucket rate limiter
│   ├── boundedqueue/     # fixed-capacity work queue
│   ├── loadshedding/     # in-flight load shedder
│   └── degradation/      # graceful degradation with fallback
├── protect/
│   ├── timeout/          # per-dependency context timeout
│   ├── retry/            # bounded retry with backoff + jitter
│   ├── circuitbreaker/   # closed/open/half-open state machine
│   └── bulkhead/         # partitioned concurrency isolation
└── internal/
    └── faildep/          # shared flaky dependency simulator (used by protect/*)
```

---

## Requirements

- **Go 1.26** — pinned via `.tool-versions` (asdf). Run `asdf install` if you haven't already.
- **No external dependencies** — stdlib only. No `go get` needed.
- **Build check:** `go build ./...`
- **Tests:** `go test ./...`
