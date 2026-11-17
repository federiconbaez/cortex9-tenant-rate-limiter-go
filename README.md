# cortex9-tenant-rate-limiter-go

Limitador de tasa por tenant basado en GCRA, con equidad ponderada entre tenants y un middleware `net/http`. Solo usa la biblioteca estándar de Go. Es un proceso único en memoria: no hay almacén distribuido.

## Algoritmo

Se usa un **GCRA jerárquico con cuota ponderada**.

El Generic Cell Rate Algorithm mantiene, por cubo, un TAT (theoretical arrival time). Una solicitud en el instante `now` es conforme si `(TAT − now) <= τ`, y en ese caso se actualiza `TAT = max(now, TAT) + T`.

- `T` es el intervalo de emisión: `Interval / tasa`.
- `τ = (burst − 1) * T`. Una ráfaga de exactamente `burst` solicitudes consecutivas se admite; la siguiente se deniega con `RetryAfter = T` (tras rellenar lo suficiente).

La equidad y el tope global se combinan así:

1. Hay una capacidad global de `Limit` solicitudes por `Interval`, con ráfaga `Burst`.
2. Cada tenant tiene un peso `w_i >= 1` (`DefaultWeight` si no se llamó a `SetWeight`).
3. El estado de un tenant se evicta si no recibe solicitudes durante `IdleTTL`. El mapa no supera `MaxTenants`: si está lleno y aparece un tenant nuevo, se evicta el de `lastSeen` más antiguo.
4. En cada `Allow` se recalcula `W` = suma de pesos de los tenants aún activos (incluido el solicitante, que se marca activo). La tasa reservada del tenant `i` es `r_i = (w_i / W) * Limit` solicitudes por `Interval`, y su ráfaga es `b_i = max(1, redondeo de (w_i / W) * Burst)`.
5. La solicitud se admite solo si el GCRA del tenant (`r_i`, `b_i`) y el GCRA global (`Limit`, `Burst`) la aceptan. Si alguno rechaza, no se actualiza el TAT del otro: no se consume cuota que no se usa.
6. Un tenant ruidoso no puede exceder su cuota justa entre los activos. La cuota de un tenant evicted se redistribuye en el siguiente `Allow`. El algoritmo **no es work-conserving de forma inmediata**: un tenant en silencio pero aún no evicted retiene su parte hasta `IdleTTL`.

El reloj es inyectable (`Clock`) para tests deterministas. Toda la mutación de estado está protegida por un mutex.

## Uso

```go
package main

import (
    "log"
    "net/http"
    "time"

    "github.com/federiconbaez/cortex9-tenant-rate-limiter-go/httplimit"
    "github.com/federiconbaez/cortex9-tenant-rate-limiter-go/limiter"
)

func main() {
    lim, err := limiter.New(limiter.Config{
        Interval: time.Second,
        Limit:    100,
        Burst:    20,
        IdleTTL:  time.Minute,
        MaxTenants: 10000,
    })
    if err != nil {
        log.Fatal(err)
    }
    _ = lim.SetWeight("tenant-premium", 3)

    mux := http.NewServeMux()
    mux.HandleFunc("/recurso", func(w http.ResponseWriter, r *http.Request) {
        _, _ = w.Write([]byte("ok"))
    })
    handler := httplimit.Middleware(lim, httplimit.Config{
        Header: "X-Tenant-ID", // valor por defecto
    })(mux)
    log.Fatal(http.ListenAndServe(":8080", handler))
}
```

La clave de tenant se toma de un encabezado (por defecto `X-Tenant-ID`). Si falta, la respuesta es `400`. Si la cuota se agota, la respuesta es `429` con `Retry-After` en segundos enteros (se redondea hacia arriba; el mínimo es 1).

## Cabeceras RateLimit

Se implementa el draft [draft-ietf-httpapi-ratelimit-headers-11](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers-11) (23 de mayo de 2026). No es un RFC: el formato puede cambiar en revisiones posteriores.

En cada respuesta con tenant válido se envían dos políticas Structured Fields, `"tenant"` (cuota justa vigente) y `"global"` (capacidad total):

```
RateLimit-Policy: "tenant";q=50;w=1, "global";q=100;w=1
RateLimit: "tenant";r=49;t=1, "global";r=99;t=1
```

- `q`: cuota de la política, en solicitudes.
- `w`: ventana en segundos enteros (el draft no admite subsegundos; si `Interval` es menor que 1 s, `w` vale 1).
- `r`: solicitudes restantes tras la decisión (ya descontada si fue admitida).
- `t`: estimación, en segundos enteros hacia arriba, del tiempo hasta recuperar el cubo lleno.

`RateLimit-Policy` puede cambiar cuando se activan o evictan tenants, porque la cuota justa se recalcula con `W`. El draft dice que el campo SHOULD permanecer consistente; aquí no se cumple en un sistema de cuotas dinámicas.

## Tests

Comando: `go test ./...` y `go test -race ./...`.

Hay 15 tests, todos pasan, también con `-race` (`15/15`).

Lo que sí está cubierto:

- GCRA determinista con `FakeClock`: ráfaga, denegación, `RetryAfter` y relleno.
- Evicción por `IdleTTL` y cota de memoria (`MaxTenants`).
- Pesos distintos y un test de equidad con un tenant ruidoso (el ruidoso queda topeado cerca de su cuota justa; el silencioso recibe casi todos sus intentos).
- Concurrencia (`Allow` y `SetWeight` juntos) con el detector de condiciones de carrera.
- Middleware con `httptest`: `200` con cabeceras del draft-11, `400` sin tenant, `429` con `Retry-After`, encabezado configurable y aislamiento entre tenants.

Lo que no está cubierto (y no se afirma):

- Varios procesos o máquinas compartiendo estado.
- Persistencia al reiniciar.
- Extracción de tenant desde JWT, cookies o la ruta; solo un encabezado.
- Cumplimiento de cabeceras frente a un parser Structured Fields de terceros.
- La equidad bajo un reloj de pared real (el test de equidad es con reloj falso y pasos de 1 ms).
- El workflow de GitHub Actions: el archivo existe, pero **CI no se puede verificar en local**.

Cobertura de sentencias, medida con `go test -coverprofile=coverage.out ./...` y `go tool cover -func=coverage.out`: **88.1 %** en total (limiter 89.0 %, httplimit 84.2 %).

## Benchmarks

Medidos con:

```
go test -bench . -benchmem ./...
```

Entorno: Go 1.27.3, `linux/amd64`, CPU `Intel(R) Xeon(R) Processor` (8 núcleos, hipervisor KVM).

```
BenchmarkAllow_singleTenant-8   	 5508751	       183.9 ns/op	       0 B/op	       0 allocs/op
BenchmarkAllow_manyTenants-8    	  607675	      1994 ns/op	       0 B/op	       0 allocs/op
BenchmarkAllow_parallel-8       	 2243917	       528.7 ns/op	       2 B/op	       1 allocs/op
```

Son números de una corrida en este entorno, no una garantía. `BenchmarkAllow_manyTenants` recorre 128 tenants (coste de evicción e iteración de pesos). En el paralelo, la asignación proviene de la clave `fmt.Sprintf` del propio banco, no del camino de un tenant ya conocido.

## CI

`.github/workflows/ci.yml` ejecuta `gofmt`, `go vet ./...` y `go test -race ./...` en GitHub Actions con Go 1.22. Ese flujo no se corrió aquí: solo se ejecutaron los mismos comandos en local.

## Limitaciones

- Un solo proceso: el estado vive en memoria y no se comparte entre réplicas. No hay backend Redis ni consenso.
- No hay persistencia. Al reiniciar, los TAT y los pesos explícitos se pierden (un tenant evicted también vuelve a `DefaultWeight`).
- No es work-conserving de inmediato: la capacidad ociosa de un tenant activo no se presta hasta `IdleTTL`.
- No hay cola. La solicitud excedentes se rechaza al instante.
- Las cabeceras `w` y `t`, y `Retry-After`, son enteros en segundos; la precisión interna puede ser subsegundo.
- `RateLimit-Policy` no es estable si cambia el conjunto de tenants activos.
- El middleware solo lee un encabezado HTTP; no autentica al tenant.
- Pensado para un servicio único, no como único control de tasa en el borde.

## Licencia

Apache-2.0. Copyright 2026 Federico Nicolas Baez Rizzo.
