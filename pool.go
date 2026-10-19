package moraine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// DefaultCompactionWorkers is the worker count NewCompactionPool uses when
// given zero or fewer.
const DefaultCompactionWorkers = 4

// poolRetryDelay is how long the pool waits before rerunning a database
// whose compaction failed.
const poolRetryDelay = 30 * time.Second

// CompactionPool runs compaction and garbage collection for many databases in
// one process on a fixed number of workers. Databases opened with a pool do
// not poll object storage: a database is queued when a flush leaves it at or
// over its L0 threshold, when its queued deletes come due, and once when it
// opens (to scan for unreferenced SSTables and catch up on compaction). When
// several databases are waiting, the one with the most L0 SSTables runs first.
//
// Close every database using the pool before closing the pool.
type CompactionPool struct {
	mu      sync.Mutex
	cond    *sync.Cond
	entries map[*Compactor]*poolEntry
	ready   map[*poolEntry]struct{}
	closed  bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type poolEntry struct {
	c       *Compactor
	l0      int // L0 count from the latest flush; orders the ready set
	running bool
	timer   *time.Timer // pending wake for due deletes or a retry
}

// NewCompactionPool starts a pool with the given number of workers.
func NewCompactionPool(workers int) *CompactionPool {
	if workers <= 0 {
		workers = DefaultCompactionWorkers
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &CompactionPool{
		entries: map[*Compactor]*poolEntry{},
		ready:   map[*poolEntry]struct{}{},
		ctx:     ctx,
		cancel:  cancel,
	}
	p.cond = sync.NewCond(&p.mu)
	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}
	return p
}

// Close stops the workers, interrupting any run in progress, and waits for
// them to exit.
func (p *CompactionPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	for _, e := range p.entries {
		if e.timer != nil {
			e.timer.Stop()
		}
	}
	p.cancel()
	p.cond.Broadcast()
	p.mu.Unlock()
	p.wg.Wait()
}

// register adds a compactor and queues its first run.
func (p *CompactionPool) register(c *Compactor, l0 int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	e := &poolEntry{c: c, l0: l0}
	p.entries[c] = e
	p.readyLocked(e)
}

// trigger queues c after a flush left it with l0 L0 SSTables.
func (p *CompactionPool) trigger(c *Compactor, l0 int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[c]; ok {
		e.l0 = l0
		p.readyLocked(e)
	}
}

// wake queues c without new L0 information, for due deletes and retries.
func (p *CompactionPool) wake(c *Compactor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.entries[c]; ok {
		p.readyLocked(e)
	}
}

func (p *CompactionPool) readyLocked(e *poolEntry) {
	if p.closed {
		return
	}
	p.ready[e] = struct{}{}
	p.cond.Signal()
}

// unregister removes c, waiting for a run in progress to finish.
func (p *CompactionPool) unregister(c *Compactor) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.entries[c]
	if !ok {
		return
	}
	p.removeLocked(e)
	for e.running {
		p.cond.Wait()
	}
}

func (p *CompactionPool) removeLocked(e *poolEntry) {
	delete(p.entries, e.c)
	delete(p.ready, e)
	if e.timer != nil {
		e.timer.Stop()
	}
}

func (p *CompactionPool) worker() {
	defer p.wg.Done()
	for {
		e := p.next()
		if e == nil {
			return
		}
		err := e.c.MaybeCompact(p.ctx)
		p.finish(e, err)
	}
}

// next blocks until a database is ready and returns the one with the most L0
// SSTables, or nil once the pool is closed. A database already running stays
// ready but is skipped until its run finishes.
func (p *CompactionPool) next() *poolEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.closed {
			return nil
		}
		var best *poolEntry
		for e := range p.ready {
			if !e.running && (best == nil || e.l0 > best.l0) {
				best = e
			}
		}
		if best != nil {
			delete(p.ready, best)
			best.running = true
			return best
		}
		p.cond.Wait()
	}
}

// finish records the end of a run and schedules the database's next wake.
func (p *CompactionPool) finish(e *poolEntry, err error) {
	var wakeAt time.Time
	if next, ok := e.c.nextDeleteAt(); ok {
		wakeAt = next
	}
	if err != nil && !errors.Is(err, ErrStaleWriter) {
		slog.Error("compaction error", "prefix", e.c.prefix, "err", err)
		if retry := e.c.now().Add(poolRetryDelay); wakeAt.IsZero() || retry.Before(wakeAt) {
			wakeAt = retry
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	e.running = false
	p.cond.Broadcast()
	if p.entries[e.c] != e {
		return // unregistered while running
	}
	if errors.Is(err, ErrStaleWriter) {
		// Another process owns the database now; it compacts it.
		p.removeLocked(e)
		return
	}
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
	if !wakeAt.IsZero() {
		c := e.c
		e.timer = time.AfterFunc(max(wakeAt.Sub(c.now()), 0), func() { p.wake(c) })
	}
}

// idle reports whether no database is ready or running.
func (p *CompactionPool) idle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.ready) > 0 {
		return false
	}
	for _, e := range p.entries {
		if e.running {
			return false
		}
	}
	return true
}
