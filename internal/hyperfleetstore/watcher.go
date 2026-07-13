package hyperfleetstore

import (
	"sync"

	storectrl "github.com/patjlm/storectrl"
)

// pollingWatcher implements storectrl.Watcher for the polling-based HyperFleet store.
// On buffer overflow the channel is closed, signaling the storectrl cache to
// reconnect via WatchFromRevision and replay missed events from the event log.
type pollingWatcher struct {
	ch     chan storectrl.Event
	mu     sync.Mutex
	closed bool
}

func newPollingWatcher(bufSize int) *pollingWatcher {
	if bufSize < 200 {
		bufSize = 200
	}
	return &pollingWatcher{
		ch: make(chan storectrl.Event, bufSize),
	}
}

// ResultChan returns the event channel for this watcher.
func (w *pollingWatcher) ResultChan() <-chan storectrl.Event {
	return w.ch
}

// Stop closes the event channel exactly once.
func (w *pollingWatcher) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		close(w.ch)
	}
}

// isStopped reports whether the watcher has been stopped.
func (w *pollingWatcher) isStopped() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

// send delivers an event to the watcher. If the channel buffer is full, the
// watcher is closed so the consumer can reconnect with WatchFromRevision.
func (w *pollingWatcher) send(event storectrl.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	select {
	case w.ch <- event:
	default:
		w.closed = true
		close(w.ch)
	}
}
