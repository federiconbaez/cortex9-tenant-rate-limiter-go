package httplimit

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/federiconbaez/cortex9-tenant-rate-limiter-go/limiter"
)

// DefaultTenantHeader es el encabezado de donde se extrae la clave de tenant.
const DefaultTenantHeader = "X-Tenant-ID"

// Config configura el middleware.
type Config struct {
	// Header es el nombre del encabezado con la clave de tenant.
	// Por defecto, X-Tenant-ID.
	Header string
}

// Middleware envuelve un handler con el limitador. Si falta el tenant,
// responde 400. Si se excede la cuota, responde 429 con Retry-After.
// En respuestas con tenant válido se envían RateLimit-Policy y RateLimit
// según draft-ietf-httpapi-ratelimit-headers-11.
func Middleware(lim *limiter.Limiter, cfg Config) func(http.Handler) http.Handler {
	if cfg.Header == "" {
		cfg.Header = DefaultTenantHeader
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := strings.TrimSpace(r.Header.Get(cfg.Header))
			if id == "" {
				http.Error(w, "falta el identificador de tenant", http.StatusBadRequest)
				return
			}
			res := lim.Allow(id)
			if res.Err == limiter.ErrEmptyTenant {
				http.Error(w, "falta el identificador de tenant", http.StatusBadRequest)
				return
			}
			writeRateLimitHeaders(w, res)
			if !res.Allowed {
				retry := ceilSeconds(res.RetryAfter)
				if retry < 1 {
					retry = 1
				}
				w.Header().Set("Retry-After", fmt.Sprintf("%d", retry))
				http.Error(w, "cuota de tasa excedida", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func writeRateLimitHeaders(w http.ResponseWriter, res limiter.Result) {
	window := windowSeconds(res.Window)
	w.Header().Set("RateLimit-Policy", formatPolicy(res.TenantQuota, res.GlobalQuota, window))
	w.Header().Set("RateLimit", formatLimit(
		res.TenantRemaining, ceilSeconds(res.TenantReset),
		res.GlobalRemaining, ceilSeconds(res.GlobalReset),
	))
}

func formatPolicy(tenantQ, globalQ, windowSec int) string {
	return fmt.Sprintf(`"tenant";q=%d;w=%d, "global";q=%d;w=%d`, tenantQ, windowSec, globalQ, windowSec)
}

func formatLimit(tenantR, tenantT, globalR, globalT int) string {
	return fmt.Sprintf(`"tenant";r=%d;t=%d, "global";r=%d;t=%d`, tenantR, tenantT, globalR, globalT)
}

func windowSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	s := int(d / time.Second)
	if s < 1 {
		return 1
	}
	return s
}

func ceilSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := int(d / time.Second)
	if d%time.Second != 0 {
		s++
	}
	return s
}
