package taskqueue

import (
	"container/list"
	"sync"
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
