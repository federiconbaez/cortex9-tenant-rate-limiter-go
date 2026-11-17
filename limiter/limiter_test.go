package limiter

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLimiter(t *testing.T, cfg Config) *Limiter {
	t.Helper()
	l, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	cases := []Config{
		{Limit: 0},
		{Limit: -1},
		{Limit: 10, Burst: -1},
		{Limit: 10, Interval: -time.Second},
	}
	for i, cfg := range cases {
		if _, err := New(cfg); err == nil {
			t.Errorf("caso %d: se esperaba error", i)
		}
	}
}

func TestGCRABurstAndRefill(t *testing.T) {
	clk := NewFakeClock(time.Unix(1_700_000_000, 0).UTC())
	l := testLimiter(t, Config{
		Interval: time.Second,
		Limit:    10,
		Burst:    3,
		Clock:    clk,
		IdleTTL:  time.Minute,
	})

	for i := 0; i < 3; i++ {
		r := l.Allow("a")
		if !r.Allowed {
			t.Fatalf("solicitud %d denegada, retry=%s", i, r.RetryAfter)
		}
	}
	denied := l.Allow("a")
	if denied.Allowed {
		t.Fatal("la cuarta solicitud inmediata debería denegarse")
	}
	if denied.RetryAfter <= 0 {
		t.Fatal("RetryAfter debería ser > 0")
	}

	clk.Advance(denied.RetryAfter)
	r := l.Allow("a")
	if !r.Allowed {
		t.Fatalf("tras RetryAfter debería admitirse, retry=%s", r.RetryAfter)
	}

	// Con ráfaga 3 y tasa 10/s, tres incrementos son 300 ms para recuperar
	// el cubo lleno desde vacío-casi-lleno. Avanzamos 1 s y debemos poder
	// repetir la ráfaga.
	clk.Advance(time.Second)
	ok := 0
	for i := 0; i < 5; i++ {
		if l.Allow("a").Allowed {
			ok++
		}
	}
	if ok != 3 {
		t.Fatalf("tras rellenar, se esperaban 3 admisiones, hubo %d", ok)
	}
}

func TestRemainingDecreasesAndQuotaHeaders(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	l := testLimiter(t, Config{Limit: 5, Burst: 5, Clock: clk})
	first := l.Allow("t")
	if !first.Allowed {
		t.Fatal("primera debería admitirse")
	}
	if first.GlobalQuota != 5 || first.TenantQuota < 1 {
		t.Fatalf("cuotas inesperadas: global=%d tenant=%d", first.GlobalQuota, first.TenantQuota)
	}
	if first.GlobalRemaining != 4 {
		t.Fatalf("GlobalRemaining: got %d want 4", first.GlobalRemaining)
	}
	second := l.Allow("t")
	if !second.Allowed {
		t.Fatal("segunda debería admitirse")
	}
	if second.GlobalRemaining >= first.GlobalRemaining {
		t.Fatalf("remaining debería bajar: %d -> %d", first.GlobalRemaining, second.GlobalRemaining)
	}
}

func TestEmptyTenant(t *testing.T) {
	l := testLimiter(t, Config{Limit: 10})
	r := l.Allow("")
	if r.Err != ErrEmptyTenant || r.Allowed {
		t.Fatalf("esperado ErrEmptyTenant, got %+v", r)
	}
	if err := l.SetWeight("", 2); err != ErrEmptyTenant {
		t.Fatalf("SetWeight: %v", err)
	}
	if err := l.SetWeight("a", 0); err != ErrInvalidWeight {
		t.Fatalf("SetWeight peso 0: %v", err)
	}
}

func TestIdleEviction(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	l := testLimiter(t, Config{
		Limit:   100,
		Burst:   100,
		IdleTTL: time.Second,
		Clock:   clk,
	})
	if !l.Allow("old").Allowed {
		t.Fatal("old")
	}
	if l.ActiveTenants() != 1 {
		t.Fatalf("activos=%d", l.ActiveTenants())
	}
	clk.Advance(time.Second + time.Millisecond)
	if !l.Allow("new").Allowed {
		t.Fatal("new")
	}
	if n := l.ActiveTenants(); n != 1 {
		t.Fatalf("tras evicción se esperaba 1 activo, hay %d", n)
	}
}

func TestMaxTenantsEvictsOldest(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	l := testLimiter(t, Config{
		Limit:      1000,
		Burst:      1000,
		MaxTenants: 2,
		IdleTTL:    time.Minute,
		Clock:      clk,
	})
	if !l.Allow("a").Allowed {
		t.Fatal("a")
	}
	clk.Advance(10 * time.Millisecond)
	if !l.Allow("b").Allowed {
		t.Fatal("b")
	}
	clk.Advance(10 * time.Millisecond)
	if !l.Allow("c").Allowed {
		t.Fatal("c")
	}
	if n := l.ActiveTenants(); n != 2 {
		t.Fatalf("activos=%d, se esperaba 2", n)
	}
	// "a" es el más antiguo y debería haber sido evicted; al volver, es nuevo
	// (no retiene TAT). Llenamos ráfaga de quienes quedaron para no depender
	// de eso: solo comprobamos cota de memoria.
}

func TestSetWeightChangesShare(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	l := testLimiter(t, Config{
		Limit:   100,
		Burst:   5,
		IdleTTL: time.Minute,
		Clock:   clk,
	})
	if err := l.SetWeight("heavy", 3); err != nil {
		t.Fatal(err)
	}
	if err := l.SetWeight("light", 1); err != nil {
		t.Fatal(err)
	}
	if !l.Allow("heavy").Allowed || !l.Allow("light").Allowed {
		t.Fatal("activación")
	}
	clk.Advance(time.Second)

	var heavy, light int
	// 2 s a paso de 1 ms: heavy reserva 75/s, light 25/s.
	for i := 0; i < 2000; i++ {
		clk.Advance(time.Millisecond)
		if l.Allow("heavy").Allowed {
			heavy++
		}
		if l.Allow("light").Allowed {
			light++
		}
	}
	if heavy < 130 || heavy > 170 {
		t.Fatalf("heavy=%d, se esperaba ~150 (75/s * 2s)", heavy)
	}
	if light < 40 || light > 60 {
		t.Fatalf("light=%d, se esperaba ~50 (25/s * 2s)", light)
	}
}

func TestFairnessNoisyTenant(t *testing.T) {
	clk := NewFakeClock(time.Unix(0, 0).UTC())
	l := testLimiter(t, Config{
		Limit:   100,
		Burst:   5,
		IdleTTL: time.Minute,
		Clock:   clk,
	})
	if !l.Allow("noisy").Allowed || !l.Allow("quiet").Allowed {
		t.Fatal("activación")
	}
	clk.Advance(time.Second)

	var noisyOK, quietOK, quietAttempts int
	const steps = 3000
	for i := 0; i < steps; i++ {
		clk.Advance(time.Millisecond)
		if l.Allow("noisy").Allowed {
			noisyOK++
		}
		if i%25 == 0 { // 40 intentos/s
			quietAttempts++
			if l.Allow("quiet").Allowed {
				quietOK++
			}
		}
	}
	if quietOK < quietAttempts-2 {
		t.Fatalf("quiet admitidas=%d de %d, debería recibir casi todos sus intentos (cuota 50/s)", quietOK, quietAttempts)
	}
	// Cuota justa del ruidoso: 50/s * 3 s = 150, más un margen de ráfaga.
	if noisyOK > 170 {
		t.Fatalf("noisy admitidas=%d, debería estar topeado cerca de 150", noisyOK)
	}
	if noisyOK < 130 {
		t.Fatalf("noisy admitidas=%d, se esperaba ~150", noisyOK)
	}
}

func TestConcurrentAllow_RaceAndProgress(t *testing.T) {
	l := testLimiter(t, Config{
		Limit:      1000,
		Burst:      1000,
		IdleTTL:    time.Minute,
		MaxTenants: 100,
	})
	const goroutines = 50
	const per = 200
	var allowed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("t%d", i%10)
			for j := 0; j < per; j++ {
				if l.Allow(id).Allowed {
					allowed.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	if allowed.Load() < 1 {
		t.Fatal("se esperaba al menos una admisión")
	}
	if n := l.ActiveTenants(); n != 10 {
		t.Fatalf("activos=%d, se esperaba 10", n)
	}
}

func TestConcurrentSetWeightAndAllow(t *testing.T) {
	l := testLimiter(t, Config{Limit: 500, Burst: 500, MaxTenants: 50})
	var wg sync.WaitGroup
	wg.Add(20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("t%d", i%5)
			for j := 0; j < 100; j++ {
				_ = l.SetWeight(id, 1+j%3)
				_ = l.Allow(id)
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkAllow_singleTenant(b *testing.B) {
	l, err := New(Config{Limit: 1_000_000_000, Burst: 1_000_000_000})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow("t1")
	}
}

func BenchmarkAllow_manyTenants(b *testing.B) {
	l, err := New(Config{Limit: 1_000_000_000, Burst: 1_000_000_000, MaxTenants: 10_000})
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]string, 128)
	for i := range ids {
		ids[i] = fmt.Sprintf("t%d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow(ids[i%len(ids)])
	}
}

func BenchmarkAllow_parallel(b *testing.B) {
	l, err := New(Config{Limit: 1_000_000_000, Burst: 1_000_000_000, MaxTenants: 100})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.Allow(fmt.Sprintf("t%d", i%16))
			i++
		}
	})
}
