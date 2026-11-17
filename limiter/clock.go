package limiter

import (
	"sync"
	"time"
)

// Clock abstrae el tiempo para que los tests puedan ser deterministas.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// FakeClock es un reloj inyectable y concurrente-seguro.
type FakeClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewFakeClock crea un reloj falso que arranca en t.
func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{t: t}
}

// Now implementa Clock.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance desplaza el reloj hacia adelante.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// Set fija el reloj a un instante absoluto.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}
