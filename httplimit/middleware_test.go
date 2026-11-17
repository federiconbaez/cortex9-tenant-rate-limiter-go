package httplimit

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/federiconbaez/cortex9-tenant-rate-limiter-go/limiter"
)

func handlerOK() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
}

func TestMiddlewareAllowsAndSetsDraft11Headers(t *testing.T) {
	lim, err := limiter.New(limiter.Config{Limit: 10, Burst: 10, Interval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(lim, Config{})(handlerOK())
	req := httptest.NewRequest(http.MethodGet, "/recurso", nil)
	req.Header.Set(DefaultTenantHeader, "acme")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	policy := rec.Header().Get("RateLimit-Policy")
	limit := rec.Header().Get("RateLimit")
	if !strings.Contains(policy, `"tenant";q=`) || !strings.Contains(policy, `"global";q=10;w=1`) {
		t.Fatalf("RateLimit-Policy inesperado: %q", policy)
	}
	if !strings.Contains(limit, `"tenant";r=`) || !strings.Contains(limit, `"global";r=`) {
		t.Fatalf("RateLimit inesperado: %q", limit)
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Fatalf("no debería haber Retry-After en 200: %q", rec.Header().Get("Retry-After"))
	}
}

func TestMiddlewareMissingTenant(t *testing.T) {
	lim, err := limiter.New(limiter.Config{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(lim, Config{})(handlerOK())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestMiddlewareCustomHeader(t *testing.T) {
	lim, err := limiter.New(limiter.Config{Limit: 10, Burst: 10})
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(lim, Config{Header: "X-Org"})(handlerOK())
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Org", "beta")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestMiddleware429RetryAfterAndHeaders(t *testing.T) {
	clk := limiter.NewFakeClock(time.Unix(0, 0).UTC())
	lim, err := limiter.New(limiter.Config{
		Limit:    1,
		Burst:    1,
		Interval: time.Second,
		Clock:    clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(lim, Config{})(handlerOK())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(DefaultTenantHeader, "acme")
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req)
	if rec1.Code != http.StatusOK {
		t.Fatalf("primera: status=%d", rec1.Code)
	}

	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("segunda: status=%d", rec2.Code)
	}
	ra := rec2.Header().Get("Retry-After")
	if ra == "" || ra == "0" {
		t.Fatalf("Retry-After inesperado: %q", ra)
	}
	if rec2.Header().Get("RateLimit-Policy") == "" || rec2.Header().Get("RateLimit") == "" {
		t.Fatalf("faltan cabeceras RateLimit en 429: policy=%q limit=%q",
			rec2.Header().Get("RateLimit-Policy"), rec2.Header().Get("RateLimit"))
	}
}

func TestMiddlewareIsolatesTenants(t *testing.T) {
	clk := limiter.NewFakeClock(time.Unix(0, 0).UTC())
	lim, err := limiter.New(limiter.Config{
		Limit:    10,
		Burst:    2,
		Interval: time.Second,
		Clock:    clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := Middleware(lim, Config{})(handlerOK())
	for _, id := range []string{"a", "b"} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set(DefaultTenantHeader, id)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("tenant %s: status=%d", id, rec.Code)
		}
	}
	// Cada tenant agotó su ráfaga de 1; una segunda de "a" debe ser 429,
	// sin impedir que documentemos el aislamiento en la primera ronda.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(DefaultTenantHeader, "a")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("segunda de a: status=%d", rec.Code)
	}
}
