package taskqueue

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// UniformPacer enforces a minimum wall-clock gap between consecutive Wait() calls.
// Used for steady request spacing (no fixed-window bursts). Decrement is a no-op so
// it matches the Limiter surface used by mesh/sound retry paths.
type UniformPacer struct {
	mu   sync.Mutex
	next time.Time
	gap  time.Duration
}

// NewUniformPacer returns a pacer with at least minGap between each Wait().
func NewUniformPacer(minGap time.Duration) *UniformPacer {
	if minGap < time.Millisecond {
		minGap = time.Millisecond
	}
	return &UniformPacer{gap: minGap}
}

func (p *UniformPacer) Wait() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if !p.next.IsZero() && now.Before(p.next) {
		time.Sleep(p.next.Sub(now))
		now = time.Now()
	}
	p.next = now.Add(p.gap)
}

// AddChill pushes the next allowed Wait time forward after a rate limit response.
func (p *UniformPacer) AddChill(d time.Duration) {
	if d <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.next.IsZero() {
		p.next = time.Now().Add(d)
		return
	}
	p.next = p.next.Add(d)
}

// Decrement is a no-op (fixedWindow decrements on some network errors).
func (p *UniformPacer) Decrement() {}

// SmoothQueue runs tasks with:
//   1) at most maxConcurrent goroutines inside the task function at once, and
//   2) at least (time.Minute / startsPerMinute) between consecutive task starts.
//
// This avoids both fixed-window edge bursts and huge stacks of concurrent operation
// polls that trigger Roblox 429 on the assets API.
type SmoothQueue[R any] struct {
	// Limiter spaces every task start and retry Wait(); Decrement is a no-op.
	Limiter *UniformPacer

	sem                chan struct{}
	mutex              sync.Mutex
	tasks              *list.List
	isSchedulerRunning bool

	// Optional anti-burst: after every breatherEvery starts, sleep breatherPauseNs (0 = off).
	// Call SetAntiBurstBreather before QueueTask. Lets the API cool between bursts without
	// changing the base pacing gap.
	breatherEvery   atomic.Int64 // N > 0: pause every N starts
	breatherPauseNs atomic.Int64 // nanoseconds
	breathCount     atomic.Uint64
}

// NewSmoothQueue creates a queue with uniform start spacing and a concurrency ceiling.
// startsPerMinute must be > 0; maxConcurrent must be > 0.
func NewSmoothQueue[R any](maxConcurrent, startsPerMinute int) *SmoothQueue[R] {
	if maxConcurrent < 1 {
		maxConcurrent = 1
	}
	if startsPerMinute < 1 {
		startsPerMinute = 1
	}
	gap := time.Minute / time.Duration(startsPerMinute)
	p := NewUniformPacer(gap)
	return &SmoothQueue[R]{
		Limiter: p,
		sem:     make(chan struct{}, maxConcurrent),
		tasks:   list.New(),
	}
}

// SetAntiBurstBreather adds a tiny sleep every N upload starts (scheduler only), after
// the normal paced Wait. Use small pause (e.g. 25–50ms) and N≈15–30 so total overhead
// stays low. Pass every < 1 or pause < 1ms to disable. Call before QueueTask.
func (q *SmoothQueue[R]) SetAntiBurstBreather(every int, pause time.Duration) {
	if every < 1 || pause < time.Millisecond {
		q.breatherEvery.Store(0)
		return
	}
	q.breatherPauseNs.Store(int64(pause)) // store before every so scheduler never sees N>0 with unset pause
	q.breatherEvery.Store(int64(every))
}

func (q *SmoothQueue[R]) QueueTask(f func() (R, error)) chan TaskResult[R] {
	c := make(chan TaskResult[R])

	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.tasks.PushBack(task[R]{
		Func: f,
		Chan: c,
	})

	if !q.isSchedulerRunning {
		q.isSchedulerRunning = true
		go q.scheduler()
	}

	return c
}

// Chill nudges the uniform pacer after Roblox signals rate limiting.
func (q *SmoothQueue[R]) Chill(d time.Duration) {
	if q.Limiter != nil {
		q.Limiter.AddChill(d)
	}
}

func (q *SmoothQueue[R]) scheduler() {
	for {
		q.mutex.Lock()
		if q.tasks.Len() == 0 {
			q.isSchedulerRunning = false
			q.mutex.Unlock()
			return
		}

		e := q.tasks.Front()
		t := e.Value.(task[R])
		q.tasks.Remove(e)
		q.mutex.Unlock()

		q.sem <- struct{}{}
		q.Limiter.Wait()
		if n := q.breatherEvery.Load(); n > 0 {
			c := q.breathCount.Add(1)
			if c%uint64(n) == 0 {
				time.Sleep(time.Duration(q.breatherPauseNs.Load()))
			}
		}
		go func(t task[R]) {
			defer func() { <-q.sem }()
			res, err := t.Func()
			t.Chan <- TaskResult[R]{
				Result: res,
				Error:  err,
			}
		}(t)
	}
}
