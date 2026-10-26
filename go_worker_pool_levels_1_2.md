# Go Worker Pool Design — Levels 1 and 2

A hands-on course from beginner to intermediate, with complete runnable examples, explanations, common mistakes, and exercises.

## Course roadmap

1. **Level 1 — Beginner:** Sequential execution, goroutines, `sync.WaitGroup`, channels, first worker pool, results.
2. **Level 2 — Intermediate:** Buffered queues, structured jobs, errors, ordering, graceful shutdown.
3. **Level 3 — Advanced (upcoming):** Context cancellation, timeouts, backpressure, retries, graceful shutdown deadlines.
4. **Level 4 — Professional (upcoming):** Reusable APIs, metrics, scaling, panic recovery.
5. **Level 5 — Expert (upcoming):** Kafka, databases, gRPC, Kubernetes, ordering and idempotency.

All examples below are **standalone programs**. Save each in its own `main.go` (or use separate directories) and run `go run main.go`. The worker schedules shown are illustrative; actual output ordering can vary.

---

# Level 1 — Beginner

## Core idea

A **worker pool** uses a fixed number of long-running goroutines to consume jobs from a shared channel. This bounds the number of jobs executing simultaneously, rather than starting a goroutine for every job.

```text
Producer --> jobs channel --> Worker 1 --\
                         --> Worker 2 ----> results channel --> Collector
                         --> Worker 3 --/
```

A worker is a goroutine, not a separate OS process or necessarily a dedicated OS thread. Concurrency does not guarantee CPU parallelism.

## Example 1 — Sequential processing

```go
package main

import (
    "fmt"
    "time"
)

func processJob(job int) {
    fmt.Printf("Processing job %d\n", job)
    time.Sleep(time.Second)
    fmt.Printf("Finished job %d\n", job)
}

func main() {
    start := time.Now()
    for job := 1; job <= 5; job++ {
        processJob(job)
    }
    fmt.Println("Total:", time.Since(start))
}
```

**Execution:** Each job waits for the preceding job. Five one-second jobs take approximately five seconds.

```text
0s    1s    2s    3s    4s    5s
| J1  | J2  | J3  | J4  | J5  |
```

**Lesson:** Simple and deterministic, but inefficient when independent jobs spend time waiting on I/O.

## Example 2 — One goroutine per job

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

func processJob(job int, wg *sync.WaitGroup) {
    defer wg.Done()
    fmt.Printf("Starting job %d\n", job)
    time.Sleep(time.Second)
    fmt.Printf("Finished job %d\n", job)
}

func main() {
    start := time.Now()
    var wg sync.WaitGroup

    for job := 1; job <= 5; job++ {
        wg.Add(1)
        go processJob(job, &wg)
    }

    wg.Wait()
    fmt.Println("Total:", time.Since(start))
}
```

**Execution:** `go` launches each function concurrently. `wg.Add(1)` registers work, `defer wg.Done()` marks completion, and `wg.Wait()` blocks until all five finish. Five independent one-second sleeps typically complete in about one second.

```text
0s           1s
| J1 --------|
| J2 --------|
| J3 --------|
| J4 --------|
| J5 --------|
```

**Tradeoff:** One goroutine per job can be excessive for millions of jobs or when downstream systems must be protected from overload.

## Example 3 — First fixed-size worker pool

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

func worker(id int, jobs <-chan int, wg *sync.WaitGroup) {
    defer wg.Done()
    for job := range jobs {
        fmt.Printf("Worker %d started job %d\n", id, job)
        time.Sleep(time.Second)
        fmt.Printf("Worker %d finished job %d\n", id, job)
    }
}

func main() {
    start := time.Now()
    const numWorkers = 3
    const numJobs = 9
    jobs := make(chan int)
    var wg sync.WaitGroup

    for w := 1; w <= numWorkers; w++ {
        wg.Add(1)
        go worker(w, jobs, &wg)
    }

    for j := 1; j <= numJobs; j++ {
        jobs <- j
    }
    close(jobs)
    wg.Wait()
    fmt.Println("Total:", time.Since(start))
}
```

**Execution:** Three worker goroutines repeatedly receive from the shared `jobs` channel. Only three jobs can be processed at once. Nine one-second jobs take approximately three seconds.

```text
Time:       0-1s       1-2s       2-3s
Worker 1:   Job 1      Job 4      Job 7
Worker 2:   Job 2      Job 5      Job 8
Worker 3:   Job 3      Job 6      Job 9
```

The exact worker/job mapping is not guaranteed.

### Why `range jobs` exits

```go
for job := range jobs {
    // process job
}
```

is equivalent in termination behavior to:

```go
for {
    job, ok := <-jobs
    if !ok {
        break
    }
    // process job
}
```

Receiving blocks when no job is available. Once the producer closes the channel **and buffered jobs are drained**, the receive reports `ok == false`, and the worker exits. The producer should generally own closing `jobs` after its final submission. Without closing, workers may remain blocked and `wg.Wait()` may never finish.

## Example 4 — Worker pool with results

```go
package main

import (
    "fmt"
    "sync"
)

type Result struct {
    JobID int
    Value int
}

func worker(id int, jobs <-chan int, results chan<- Result, wg *sync.WaitGroup) {
    defer wg.Done()
    for job := range jobs {
        results <- Result{JobID: job, Value: job * job}
    }
}

func main() {
    jobs := make(chan int, 5)
    results := make(chan Result, 5)
    var wg sync.WaitGroup

    for w := 1; w <= 3; w++ {
        wg.Add(1)
        go worker(w, jobs, results, &wg)
    }

    for j := 1; j <= 5; j++ {
        jobs <- j
    }
    close(jobs)

    go func() {
        wg.Wait()
        close(results)
    }()

    for result := range results {
        fmt.Printf("Job %d -> %d\n", result.JobID, result.Value)
    }
}
```

**Execution:** Workers read from `jobs` and send squared values to `results`. A separate coordinator goroutine waits for all workers, then closes `results`. Meanwhile, main drains results until that channel closes. The order of results is not guaranteed.

**Directional channels:** `jobs <-chan int` is receive-only to the worker; `results chan<- Result` is send-only. This makes intent explicit and prevents misuse at compile time.

**Important:** The background coordinator waits at `wg.Wait()`, but main continues consuming `results`. When workers finish, `close(results)` allows the collector's `range` to terminate after remaining buffered results have been received.

### Level 1 common mistakes

- Forgetting `wg.Done()` means `wg.Wait()` may never return.
- Forgetting to close the jobs channel may leave workers blocked forever.
- Closing a channel while senders are active causes `panic: send on closed channel`.
- Assuming results arrive in submission order is incorrect.
- Launching an unbounded goroutine per job can exhaust resources or overload dependencies.

### Level 1 exercises

- [ ] **A — Even and odd numbers:** Create 2 workers and submit integers 1–20. Each worker prints the number and whether it is even or odd.
- [ ] **B — Square calculator:** Create 4 workers and 10 jobs. Collect each square and calculate the sum of all squares.
- [ ] **C — Simulated API requests:** Create 3 workers processing 15 requests. Each request takes 500 ms. Compare sequential time with worker-pool time.
- [ ] **D — Preserve result order:** Extend Example 4 to print results in submission order, regardless of completion order.

---

# Level 2 — Intermediate

## Concepts: buffered vs. unbuffered channels

```go
unbuffered := make(chan int)      // capacity 0
buffered   := make(chan int, 10)  // capacity 10
```

| Feature | Unbuffered | Buffered |
|---|---|---|
| Send blocks | Until a receiver is ready | When buffer is full |
| Receive blocks | Until a sender is ready | When buffer is empty |
| Pending queue capacity | 0 | Declared capacity |
| Main benefit | Direct handoff | Decouple producer and consumers temporarily |

A queue of 100 and 5 workers does **not** mean 105 jobs execute simultaneously: only 5 workers can actively process jobs. Buffering can improve producer responsiveness, but does not automatically improve processing throughput.

## Example 1 — Buffered vs. unbuffered queue

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

func runPool(bufferSize int) {
    const workers = 3
    const totalJobs = 9
    jobs := make(chan int, bufferSize)
    var wg sync.WaitGroup
    start := time.Now()

    for w := 1; w <= workers; w++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for job := range jobs {
                fmt.Printf("Worker %d processing job %d\n", id, job)
                time.Sleep(300 * time.Millisecond)
            }
        }(w)
    }

    for j := 1; j <= totalJobs; j++ {
        jobs <- j
    }
    submitted := time.Since(start)
    close(jobs)
    wg.Wait()

    fmt.Printf("Buffer=%d, submission=%v, total=%v\n",
        bufferSize, submitted, time.Since(start))
}

func main() {
    runPool(0)
    runPool(9)
}
```

**Execution:** With buffer 0, the producer must wait for a worker to receive each send. With buffer 9, all jobs can be queued quickly. Both cases have 3 workers, so nine 300-ms jobs take roughly 900 ms overall, ignoring overhead. Submission times differ, but the precise timings vary by scheduler.

**Exercise in thought:** What happens if workers are slow, the queue has capacity 2, and 100 jobs arrive at once? Once the queue fills, the producer blocks until a worker takes another job. This is basic **backpressure**.

## Example 2 — Structured jobs and results

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

type Job struct {
    ID    int
    Input int
}

type Result struct {
    JobID    int
    WorkerID int
    Output   int
}

func worker(id int, jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
    defer wg.Done()
    for job := range jobs {
        time.Sleep(200 * time.Millisecond)
        results <- Result{
            JobID: job.ID, WorkerID: id, Output: job.Input * job.Input,
        }
    }
}

func main() {
    const numWorkers = 3
    const numJobs = 8
    jobs := make(chan Job, numJobs)
    results := make(chan Result, numJobs)
    var wg sync.WaitGroup

    for w := 1; w <= numWorkers; w++ {
        wg.Add(1)
        go worker(w, jobs, results, &wg)
    }

    for j := 1; j <= numJobs; j++ {
        jobs <- Job{ID: j, Input: j * 10}
    }
    close(jobs)

    go func() {
        wg.Wait()
        close(results)
    }()

    for result := range results {
        fmt.Printf("Worker %d: Job %d => %d\n",
            result.WorkerID, result.JobID, result.Output)
    }
}
```

**Execution:** Structured jobs carry their IDs and inputs. Results carry job ID, worker ID, and output. This is a foundation for real workloads such as HTTP requests, file processing, and database operations.

**Why close results in a background goroutine?** Main must consume results while workers produce them. If main calls `wg.Wait()` before receiving, workers may block on a full results channel and never finish.

## Example 3 — Per-job errors and ordered output

```go
package main

import (
    "fmt"
    "sort"
    "sync"
    "time"
)

type Job struct {
    ID          int
    AmountCents int64
}

type Result struct {
    JobID  int
    Status string
    Err    error
}

func process(job Job) Result {
    time.Sleep(100 * time.Millisecond)
    if job.AmountCents <= 0 {
        return Result{JobID: job.ID,
            Err: fmt.Errorf("invalid amount: %d", job.AmountCents)}
    }
    if job.AmountCents > 100000 {
        return Result{JobID: job.ID,
            Err: fmt.Errorf("amount exceeds limit")}
    }
    return Result{JobID: job.ID, Status: "APPROVED"}
}

func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
    defer wg.Done()
    for job := range jobs {
        results <- process(job)
    }
}

func main() {
    input := []Job{
        {ID: 1, AmountCents: 5000},
        {ID: 2, AmountCents: -100},
        {ID: 3, AmountCents: 7500},
        {ID: 4, AmountCents: 150000},
        {ID: 5, AmountCents: 2500},
    }
    jobs := make(chan Job)
    results := make(chan Result)
    var wg sync.WaitGroup

    for i := 0; i < 3; i++ {
        wg.Add(1)
        go worker(jobs, results, &wg)
    }

    go func() {
        for _, job := range input {
            jobs <- job
        }
        close(jobs)
    }()

    go func() {
        wg.Wait()
        close(results)
    }()

    var collected []Result
    for result := range results {
        collected = append(collected, result)
    }
    sort.Slice(collected, func(i, j int) bool {
        return collected[i].JobID < collected[j].JobID
    })

    for _, result := range collected {
        if result.Err != nil {
            fmt.Printf("Job %d FAILED: %v\n", result.JobID, result.Err)
        } else {
            fmt.Printf("Job %d: %s\n", result.JobID, result.Status)
        }
    }
}
```

Expected final output:

```text
Job 1: APPROVED
Job 2 FAILED: invalid amount: -100
Job 3: APPROVED
Job 4 FAILED: amount exceeds limit
Job 5: APPROVED
```

**Execution:** Individual jobs can fail without stopping other workers. Each result contains an `Err` field. The collector gathers all results and sorts them by ID before printing. Sorting by ID works here because IDs are unique and increasing; in general, store a separate input `Index` to restore submission order when IDs are arbitrary or duplicated.

**Financial note:** Integer cents avoid binary floating-point rounding issues for simple money amounts.

**Failure categories:** A job-level failure (invalid input) is distinct from a pool-level failure (shutdown, canceled context, infrastructure failure).

## Example 4 — Reusable pool with safe shutdown

```go
package main

import (
    "errors"
    "fmt"
    "sync"
    "time"
)

var ErrClosed = errors.New("worker pool is closed")

type Pool struct {
    jobs    chan int
    workers sync.WaitGroup
    mu      sync.Mutex
    closed  bool
}

func NewPool(workerCount, queueSize int) *Pool {
    if workerCount <= 0 {
        panic("workerCount must be positive")
    }
    if queueSize < 0 {
        panic("queueSize cannot be negative")
    }

    p := &Pool{jobs: make(chan int, queueSize)}
    for i := 1; i <= workerCount; i++ {
        p.workers.Add(1)
        go func(id int) {
            defer p.workers.Done()
            for job := range p.jobs {
                fmt.Printf("Worker %d processing job %d\n", id, job)
                time.Sleep(200 * time.Millisecond)
            }
        }(i)
    }
    return p
}

func (p *Pool) Submit(job int) error {
    p.mu.Lock()
    defer p.mu.Unlock()
    if p.closed {
        return ErrClosed
    }
    p.jobs <- job
    return nil
}

func (p *Pool) Shutdown() {
    p.mu.Lock()
    if !p.closed {
        p.closed = true
        close(p.jobs)
    }
    p.mu.Unlock()
    p.workers.Wait()
}

func main() {
    pool := NewPool(3, 5)
    for i := 1; i <= 10; i++ {
        if err := pool.Submit(i); err != nil {
            fmt.Println("Submit failed:", err)
        }
    }
    pool.Shutdown()
    pool.Shutdown() // idempotent
    fmt.Println("After shutdown:", pool.Submit(11))
}
```

**Execution:** `NewPool` starts workers; `Submit` checks whether the pool is closed and sends the job; `Shutdown` closes the jobs channel exactly once and waits for workers to drain accepted jobs. Submissions after shutdown return `ErrClosed`.

**Why the mutex?** Without coordination, a concurrent `Submit` could attempt to send to a channel just closed by `Shutdown`, causing a panic. The mutex serializes submission and closure.

**Limitation (important):** This teaching implementation holds the mutex while sending. When the queue is full, `Submit` blocks with the mutex held and `Shutdown` cannot acquire it until submission progresses. Workers must continue draining the queue. It does **not** support context cancellation, deadline-bound shutdown, or nonblocking admission. These are Level 3 topics. It is safe for the shown assumptions, not a complete production library.

## Deadlock walkthrough

The following code is intentionally **incorrect**:

```go
jobs := make(chan int)
results := make(chan int)
var wg sync.WaitGroup
wg.Add(1)

go func() {
    defer wg.Done()
    for job := range jobs {
        results <- job * job
    }
}()

jobs <- 10
close(jobs)
wg.Wait()              // main waits for worker
fmt.Println(<-results) // main won't receive yet
```

```text
Main goroutine                 Worker goroutine
wg.Wait()                      results <- 100
    |                                |
    +---- waits for worker           +---- waits for receiver
                     DEADLOCK
```

**Fix:** Consume results concurrently with workers, then close results only after all workers have exited. A coordinator goroutine can do `wg.Wait(); close(results)` while main does `for result := range results`.

## Choosing worker counts

For equal-duration independent jobs, idealized throughput is approximately:

\[
\text{throughput} \approx \frac{\text{number of workers}}{\text{average job duration}}
\]

If each job takes 0.2 seconds, 10 workers suggest roughly 50 jobs/second; 50 workers suggest roughly 250 jobs/second **only if** resources and downstream systems permit it.

For CPU-bound work, starting around `runtime.GOMAXPROCS(0)` workers is reasonable. For I/O-bound work, higher concurrency may help. Always benchmark and observe resource limits, downstream rate limits, queue depth, and latency.

## Level 2 common mistakes

| Mistake | Consequence | Better design |
|---|---|---|
| Close results before workers finish | Send-on-closed-channel panic | Close after `wg.Wait()` |
| Wait before draining unbuffered/full results | Deadlock | Drain concurrently |
| Unbounded queue | Memory pressure | Bounded buffer and admission policy |
| Assume result order | Misassociated responses | Carry job ID or input index |
| Ignore errors | Silent partial failures | Return per-job `Err` |
| Multiple channel closers | Double-close panic | Single owner/coordinator |
| Race shutdown against submission | Send-on-closed panic | Coordinate lifecycle |
| Too many workers | Contention/downstream overload | Benchmark and monitor |

## Race detector and repeated tests

```bash
go run -race main.go
go test -race ./...
go test -race -count=100 ./...
```

The race detector identifies data races, but does not prove freedom from deadlocks or goroutine leaks. Test with queue sizes 0 and 1 and concurrent submit/shutdown scenarios.

### Level 2 exercises

- [ ] **A — File processing pool:** Create 4 workers and 20 simulated file jobs. Each job has `FileName`, `Size`, and `ID`. Return the processing duration.
- [ ] **B — Database batch simulator:** Create 5 workers to process 50 records. Fail every 7th record; report success and failure counts.
- [ ] **C — Ordered results:** Create 3 workers processing 15 jobs with different delays. Print results in exact input order.
- [ ] **D — Concurrent shutdown:** Submit jobs from 5 producer goroutines while another goroutine calls `Shutdown`. Accepted jobs should finish; rejected jobs return `ErrClosed`; no channel panic should occur. Test with `-race`.

## Level 2 knowledge check

1. What happens when a buffered channel is full and a goroutine sends another job?
2. Who should close the results channel, and when?
3. Does a pool with 5 workers and a queue capacity of 100 execute 105 jobs simultaneously?
4. Why can `wg.Wait()` before collecting results cause a deadlock?

<details>
<summary>Answers</summary>

1. The sender blocks until buffer space becomes available (unless an alternative `select` branch applies).
2. A coordinator closes it after **all** workers finish sending.
3. No. At most 5 worker jobs execute simultaneously; 100 more may be queued.
4. Workers can block sending to results while main waits for workers to finish; neither can progress.

</details>

---

# Quick reference

```text
Producer owns jobs channel closure.
Workers receive jobs and send results.
Coordinator waits for workers, then closes results.
Collector drains results until closed.
```

```go
// Canonical pattern:
for i := 0; i < workerCount; i++ {
    wg.Add(1)
    go worker(jobs, results, &wg)
}

go func() {
    defer close(jobs)
    for _, job := range input {
        jobs <- job
    }
}()

go func() {
    wg.Wait()
    close(results)
}()

for result := range results {
    // handle result
}
```

**Note:** This skeleton assumes `wg` is a `sync.WaitGroup`, workers are started before the coordinator, and no other goroutine closes `jobs` or `results`.

## Next: Level 3 — Advanced

We will improve admission and lifecycle control with `context.Context`, deadlines, cancellation, bounded waiting, retries with backoff, and goroutine leak prevention. The goal is to move from a teaching pool to a robust service component.
