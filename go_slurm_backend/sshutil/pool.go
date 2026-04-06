package sshutil

import (
	"fmt"
	"sync"
	"time"

	"go-slurm-backend/api"
)

// poolEntry holds a cached SSHExecutor and its last-used timestamp.
type poolEntry struct {
	exec     *SSHExecutor
	lastUsed time.Time
}

// Pool manages a cache of SSHExecutor instances keyed by (host, port, user).
// Idle connections are reaped after idleTimeout.
type Pool struct {
	mu          sync.Mutex
	entries     map[string]*poolEntry
	idleTimeout time.Duration
	stopReaper  chan struct{}
}

// NewPool creates a connection pool that reaps idle connections every
// idleTimeout/2.  Call Close to stop the reaper and close all connections.
func NewPool(idleTimeout time.Duration) *Pool {
	p := &Pool{
		entries:     make(map[string]*poolEntry),
		idleTimeout: idleTimeout,
		stopReaper:  make(chan struct{}),
	}
	go p.reapLoop()
	return p
}

func poolKey(conf api.SshConfig) string {
	port := conf.Port
	if port == 0 {
		port = 22
	}
	return fmt.Sprintf("%s@%s:%d", conf.User, conf.IpAddr, port)
}

// Get returns a cached SSHExecutor for the given config, or creates a new one.
// The returned executor must be released with Put when the caller is done.
func (p *Pool) Get(conf api.SshConfig) (*SSHExecutor, error) {
	key := poolKey(conf)

	p.mu.Lock()
	if entry, ok := p.entries[key]; ok {
		entry.lastUsed = time.Now()
		exec := entry.exec
		p.mu.Unlock()
		// Verify the connection is still alive with a lightweight probe.
		if _, err := exec.Exec("true"); err == nil {
			return exec, nil
		}
		// Connection is dead; remove and create a fresh one.
		p.mu.Lock()
		delete(p.entries, key)
		exec.Close()
		p.mu.Unlock()
	} else {
		p.mu.Unlock()
	}

	exec, err := NewSSHExecutor(conf)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.entries[key] = &poolEntry{exec: exec, lastUsed: time.Now()}
	p.mu.Unlock()
	return exec, nil
}

// Put marks the executor as no longer in active use.  Currently a no-op
// (the connection stays pooled); exists for future use-counting.
func (p *Pool) Put(_ *SSHExecutor) {}

func (p *Pool) reapLoop() {
	ticker := time.NewTicker(p.idleTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.reapIdle()
		case <-p.stopReaper:
			return
		}
	}
}

func (p *Pool) reapIdle() {
	now := time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.entries {
		if now.Sub(entry.lastUsed) > p.idleTimeout {
			entry.exec.Close()
			delete(p.entries, key)
		}
	}
}

// Close stops the reaper and closes all pooled connections.
func (p *Pool) Close() {
	close(p.stopReaper)
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.entries {
		entry.exec.Close()
		delete(p.entries, key)
	}
}
