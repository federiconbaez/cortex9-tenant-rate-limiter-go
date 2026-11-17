package limiter

import "time"

// gcra implementa el Generic Cell Rate Algorithm sobre un único cubo.
//
// Parámetros: increment T (intervalo de emisión) y tolerancia τ = burst * T.
// Una llegada en `now` es conforme si (TAT − now) < τ cuando TAT es futuro,
// o siempre si TAT no está en el futuro. El rechazo usa >= τ para que una
// ráfaga de exactamente `burst` solicitudes consecutivas sea aceptada y la
// siguiente se deniegue.
type gcra struct {
	tat time.Time
}

func emissionInterval(interval time.Duration, limit float64) time.Duration {
	if limit <= 0 {
		return 24 * time.Hour
	}
	ns := float64(interval) / limit
	const maxNS = float64(24 * time.Hour)
	if ns > maxNS {
		return 24 * time.Hour
	}
	if ns < 1 {
		return time.Nanosecond
	}
	return time.Duration(ns)
}

// delayTolerance es τ = (burst-1)*T. Así, una ráfaga de exactamente `burst`
// solicitudes es conforme y la siguiente se deniega con RetryAfter = T.
func delayTolerance(increment time.Duration, burst float64) time.Duration {
	if burst <= 1 || increment <= 0 {
		return 0
	}
	ns := float64(increment) * (burst - 1)
	const maxNS = float64(24 * time.Hour * 7)
	if ns > maxNS {
		return 24 * time.Hour * 7
	}
	return time.Duration(ns)
}

func (g *gcra) ahead(now time.Time) time.Duration {
	if g.tat.IsZero() || !g.tat.After(now) {
		return 0
	}
	return g.tat.Sub(now)
}

func (g *gcra) conforms(now time.Time, tau time.Duration) (ok bool, retryAfter time.Duration) {
	a := g.ahead(now)
	if a > tau {
		return false, a - tau
	}
	return true, 0
}

func (g *gcra) commit(now time.Time, increment time.Duration) {
	base := now
	if !g.tat.IsZero() && g.tat.After(now) {
		base = g.tat
	}
	g.tat = base.Add(increment)
}

func remainingTokens(ahead, increment time.Duration, burst float64) float64 {
	if burst <= 0 {
		return 0
	}
	if increment <= 0 {
		return 0
	}
	rem := burst - float64(ahead)/float64(increment)
	if rem < 0 {
		return 0
	}
	if rem > burst {
		return burst
	}
	return rem
}
