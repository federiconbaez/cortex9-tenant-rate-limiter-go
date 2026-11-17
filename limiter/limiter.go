package limiter

import (
	"errors"
	"sync"
	"time"
)

var (
	// ErrEmptyTenant se devuelve cuando el identificador de tenant está vacío.
	ErrEmptyTenant = errors.New("el identificador de tenant está vacío")
	// ErrInvalidWeight se devuelve cuando el peso es menor que 1.
	ErrInvalidWeight = errors.New("el peso del tenant debe ser mayor o igual a 1")
	// ErrInvalidConfig se devuelve cuando Config no es utilizable.
	ErrInvalidConfig = errors.New("la configuración del limitador no es válida")
)

const (
	defaultInterval      = time.Second
	defaultIdleTTL       = time.Minute
	defaultMaxTenants    = 10_000
	defaultDefaultWeight = 1
)

// Config describe la capacidad global y los límites de memoria del limitador.
type Config struct {
	// Interval es la ventana de la tasa global. Por defecto, 1 s.
	Interval time.Duration
	// Limit es la capacidad global: solicitudes admitidas por Interval.
	// Debe ser mayor que 0.
	Limit int
	// Burst es la ráfaga global (solicitudes consecutivas admitidas partiendo
	// de un cubo lleno). Si es 0, se usa Limit.
	Burst int
	// DefaultWeight es el peso de un tenant sin peso explícito. Por defecto, 1.
	DefaultWeight int
	// IdleTTL es el tiempo sin solicitudes tras el cual se evicta el estado
	// de un tenant. Por defecto, 1 minuto.
	IdleTTL time.Duration
	// MaxTenants acota la cantidad de tenants con estado en memoria.
	// Si se alcanza y llega uno nuevo, se evicta el de lastSeen más antiguo.
	// Por defecto, 10_000.
	MaxTenants int
	// Clock, si no es nil, reemplaza el reloj de pared. Útil en tests.
	Clock Clock
}

func (c Config) withDefaults() (Config, error) {
	if c.Interval < 0 || c.Limit <= 0 || c.Burst < 0 || c.DefaultWeight < 0 || c.IdleTTL < 0 || c.MaxTenants < 0 {
		return Config{}, ErrInvalidConfig
	}
	if c.Interval == 0 {
		c.Interval = defaultInterval
	}
	if c.Burst == 0 {
		c.Burst = c.Limit
	}
	if c.DefaultWeight == 0 {
		c.DefaultWeight = defaultDefaultWeight
	}
	if c.IdleTTL == 0 {
		c.IdleTTL = defaultIdleTTL
	}
	if c.MaxTenants == 0 {
		c.MaxTenants = defaultMaxTenants
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.DefaultWeight < 1 || c.Burst < 1 || c.MaxTenants < 1 {
		return Config{}, ErrInvalidConfig
	}
	return c, nil
}

// Result es el resultado de Allow.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration
	// Err es distinto de nil solo para entradas inválidas (tenant vacío).
	Err error

	// TenantQuota y GlobalQuota son las cuotas enteras vigentes (q del draft).
	TenantQuota int
	GlobalQuota int
	// TenantRemaining y GlobalRemaining son las unidades restantes tras la
	// decisión (r del draft), ya descontada la solicitud si fue admitida.
	TenantRemaining int
	GlobalRemaining int
	// TenantReset y GlobalReset estiman el tiempo hasta recuperar el cubo
	// lleno (base de t del draft, en duración).
	TenantReset time.Duration
	GlobalReset time.Duration
	// Window es Interval, base de w del draft.
	Window time.Duration
}

// Limiter es un limitador GCRA por tenant con equidad ponderada y un tope global.
// Es seguro para uso concurrente.
//
// Algoritmo (GCRA jerárquico con cuota ponderada):
//
//  1. Hay una capacidad global de Limit solicitudes por Interval, con ráfaga Burst.
//  2. Cada tenant tiene un peso w_i >= 1 (DefaultWeight si no se llamó a SetWeight).
//  3. El estado de un tenant se evicta si no recibe solicitudes durante IdleTTL.
//     El mapa de tenants no supera MaxTenants: si está lleno y aparece uno nuevo,
//     se evicta el de lastSeen más antiguo.
//  4. En cada Allow se recalcula W = suma de pesos de los tenants aún activos
//     (incluyendo al solicitante, que se marca activo). La tasa reservada del
//     tenant i es r_i = (w_i / W) * Limit solicitudes por Interval, y su ráfaga
//     es b_i = max(1, redondeo de (w_i / W) * Burst).
//  5. La solicitud se admite solo si el GCRA del tenant (tasa r_i, ráfaga b_i)
//     y el GCRA global (tasa Limit, ráfaga Burst) la aceptan. Si alguno
//     rechaza, no se actualiza el TAT del otro: no se consume cuota no usada.
//  6. Un tenant ruidoso no puede exceder su cuota justa entre los activos.
//     La cuota de un tenant evicted se redistribuye en el siguiente Allow.
//     El algoritmo no es work-conserving de forma inmediata: un tenant en
//     silencio pero aún no evicted retiene su parte hasta IdleTTL.
type Limiter struct {
	mu      sync.Mutex
	cfg     Config
	global  gcra
	tenants map[string]*tenantState
}

type tenantState struct {
	weight   int
	lastSeen time.Time
	bucket   gcra
}

// New construye un Limiter. Limit debe ser mayor que 0.
func New(cfg Config) (*Limiter, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Limiter{
		cfg:     cfg,
		tenants: make(map[string]*tenantState),
	}, nil
}

// SetWeight asigna un peso w >= 1 al tenant. Si el tenant no tiene estado,
// se crea (cuenta para MaxTenants y para W desde ese momento). Tras una
// evicción, el peso vuelve a DefaultWeight.
func (l *Limiter) SetWeight(id string, w int) error {
	if id == "" {
		return ErrEmptyTenant
	}
	if w < 1 {
		return ErrInvalidWeight
	}
	now := l.cfg.Clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evictIdleLocked(now)
	t, err := l.getOrCreateLocked(id, now)
	if err != nil {
		return err
	}
	t.weight = w
	t.lastSeen = now
	return nil
}

// ActiveTenants devuelve cuántos tenants tienen estado en memoria.
func (l *Limiter) ActiveTenants() int {
	now := l.cfg.Clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evictIdleLocked(now)
	return len(l.tenants)
}

// Allow evalúa una solicitud de una unidad para el tenant id.
func (l *Limiter) Allow(id string) Result {
	if id == "" {
		return Result{Err: ErrEmptyTenant, Window: l.window()}
	}
	now := l.cfg.Clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.evictIdleLocked(now)

	t, err := l.getOrCreateLocked(id, now)
	if err != nil {
		// Sin cupo de memoria: no admitimos a un tenant nuevo si no pudimos
		// evictar (no debería ocurrir: evictamos el más antiguo). Se informa
		// como no permitido.
		return Result{Allowed: false, Err: err, Window: l.cfg.Interval, GlobalQuota: l.cfg.Limit}
	}

	wsum := l.weightSumLocked()
	if wsum < 1 {
		wsum = t.weight
	}
	share := float64(t.weight) / float64(wsum)
	tenantLimit := share * float64(l.cfg.Limit)
	tenantBurst := share * float64(l.cfg.Burst)
	if tenantBurst < 1 {
		tenantBurst = 1
	}

	tInc := emissionInterval(l.cfg.Interval, tenantLimit)
	tTau := delayTolerance(tInc, tenantBurst)
	gInc := emissionInterval(l.cfg.Interval, float64(l.cfg.Limit))
	gTau := delayTolerance(gInc, float64(l.cfg.Burst))

	tOK, tRetry := t.bucket.conforms(now, tTau)
	gOK, gRetry := l.global.conforms(now, gTau)

	res := Result{
		TenantQuota: intRoundAtLeast(tenantLimit, 1),
		GlobalQuota: l.cfg.Limit,
		Window:      l.cfg.Interval,
	}

	if tOK && gOK {
		t.bucket.commit(now, tInc)
		l.global.commit(now, gInc)
		t.lastSeen = now
		res.Allowed = true
		res.TenantRemaining = intFloor(remainingTokens(t.bucket.ahead(now), tInc, tenantBurst))
		res.GlobalRemaining = intFloor(remainingTokens(l.global.ahead(now), gInc, float64(l.cfg.Burst)))
		res.TenantReset = t.bucket.ahead(now)
		res.GlobalReset = l.global.ahead(now)
		return res
	}

	retry := tRetry
	if !gOK && (!tOK && gRetry > tRetry || tOK) {
		retry = gRetry
	}
	if !tOK && !gOK && gRetry > tRetry {
		retry = gRetry
	}
	res.Allowed = false
	res.RetryAfter = retry
	res.TenantRemaining = intFloor(remainingTokens(t.bucket.ahead(now), tInc, tenantBurst))
	res.GlobalRemaining = intFloor(remainingTokens(l.global.ahead(now), gInc, float64(l.cfg.Burst)))
	res.TenantReset = t.bucket.ahead(now)
	res.GlobalReset = l.global.ahead(now)
	// El tenant sigue activo: un rechazo también cuenta como visto, para que
	// un cliente ruidoso no desaparezca de W y se le asigne más cuota.
	t.lastSeen = now
	return res
}

func (l *Limiter) window() time.Duration {
	if l == nil || l.cfg.Interval == 0 {
		return defaultInterval
	}
	return l.cfg.Interval
}

func (l *Limiter) weightSumLocked() int {
	sum := 0
	for _, t := range l.tenants {
		sum += t.weight
	}
	return sum
}

func (l *Limiter) evictIdleLocked(now time.Time) {
	ttl := l.cfg.IdleTTL
	for id, t := range l.tenants {
		if now.Sub(t.lastSeen) > ttl {
			delete(l.tenants, id)
		}
	}
}

func (l *Limiter) getOrCreateLocked(id string, now time.Time) (*tenantState, error) {
	if t, ok := l.tenants[id]; ok {
		return t, nil
	}
	if len(l.tenants) >= l.cfg.MaxTenants {
		l.evictOldestLocked()
	}
	if len(l.tenants) >= l.cfg.MaxTenants {
		return nil, ErrInvalidConfig
	}
	t := &tenantState{weight: l.cfg.DefaultWeight, lastSeen: now}
	l.tenants[id] = t
	return t, nil
}

func (l *Limiter) evictOldestLocked() {
	var oldestID string
	var oldest time.Time
	first := true
	for id, t := range l.tenants {
		if first || t.lastSeen.Before(oldest) {
			oldestID = id
			oldest = t.lastSeen
			first = false
		}
	}
	if oldestID != "" {
		delete(l.tenants, oldestID)
	}
}

func intFloor(v float64) int {
	if v <= 0 {
		return 0
	}
	return int(v)
}

func intRoundAtLeast(v float64, min int) int {
	n := int(v + 0.5)
	if n < min {
		return min
	}
	return n
}
