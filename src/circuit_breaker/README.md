# Circuit Breaker Module

Implementación genérica y reutilizable del patrón **Circuit Breaker** para proteger sistemas distribuidos contra fallos en cascada.

## 🎯 Características

- ✅ **Completamente agnóstico**: No depende de Kafka, HTTP, DB o cualquier tecnología específica
- ✅ **API simple**: `allow()`, `record_success()`, `record_failure()`
- ✅ **Estados clásicos**: CLOSED → OPEN → HALF_OPEN → CLOSED
- ✅ **Thread-safe**: Usa `parking_lot::RwLock` para concurrencia
- ✅ **Sin async innecesario**: Operaciones síncronas y eficientes
- ✅ **Altamente testeable**: 11 tests unitarios incluidos

## 📐 Arquitectura

```
┌──────────────────────────────────────────────────┐
│ circuit_breaker/                                 │
│                                                  │
│  ├── mod.rs       → API pública y documentación │
│  ├── breaker.rs   → Lógica del circuit breaker  │
│  └── state.rs     → Estados y transiciones      │
└──────────────────────────────────────────────────┘
```

## 🔄 Diagrama de Estados

```
                    ┌─────────────┐
         ┌─────────►│   CLOSED    │◄──────────┐
         │          └─────────────┘           │
         │                 │                  │
         │                 │ failures ≥       │
         │                 │ threshold        │
         │                 ▼                  │
         │          ┌─────────────┐           │
         │          │    OPEN     │           │
         │          └─────────────┘           │
         │                 │                  │
         │                 │ timeout          │
         │                 │ expired          │
         │                 ▼                  │
         │          ┌─────────────┐           │
         │   fail   │ HALF_OPEN   │ success × 2
         └──────────┤             ├───────────┘
                    └─────────────┘
```

## 🚀 Uso Básico

### Importar el módulo

```rust
use geocontext::circuit_breaker::{CircuitBreaker, Config};
use std::time::Duration;
```

### Crear un Circuit Breaker

```rust
// Configuración: 5 fallos consecutivos, 30 segundos de timeout
let config = Config::new(5, Duration::from_secs(30));
let breaker = CircuitBreaker::new("external-api", config);
```

### Proteger una operación

```rust
if breaker.allow() {
    match external_call() {
        Ok(result) => {
            breaker.record_success();
            // Usar result
        }
        Err(e) => {
            breaker.record_failure();
            // Manejar error
        }
    }
} else {
    // Circuit está OPEN, no ejecutar operación
    println!("Service unavailable, circuit is open");
}
```

## 📋 API Completa

### `Config`

```rust
impl Config {
    /// Crea configuración con threshold y timeout
    pub fn new(failure_threshold: usize, reset_timeout: Duration) -> Self;
    
    /// Configura éxitos necesarios en HALF_OPEN (default: 2)
    pub fn with_success_threshold(self, threshold: usize) -> Self;
}
```

**Ejemplo**:
```rust
let config = Config::new(3, Duration::from_secs(10))
    .with_success_threshold(3);
```

### `CircuitBreaker`

```rust
impl CircuitBreaker {
    /// Crea un nuevo circuit breaker
    pub fn new(name: impl Into<String>, config: Config) -> Self;
    
    /// Verifica si se permite ejecutar una operación
    pub fn allow(&self) -> bool;
    
    /// Registra una ejecución exitosa
    pub fn record_success(&self);
    
    /// Registra una ejecución fallida
    pub fn record_failure(&self);
    
    /// Obtiene el estado actual (CLOSED | OPEN | HALF_OPEN)
    pub fn state(&self) -> State;
    
    /// Obtiene el nombre del breaker
    pub fn name(&self) -> &str;
    
    /// Resetea manualmente a CLOSED
    pub fn reset(&self);
}
```

## 💡 Ejemplos de Uso

### Proteger llamadas HTTP

```rust
use geocontext::circuit_breaker::{CircuitBreaker, Config};
use std::time::Duration;

struct HttpClient {
    breaker: CircuitBreaker,
}

impl HttpClient {
    fn new() -> Self {
        let config = Config::new(5, Duration::from_secs(30));
        Self {
            breaker: CircuitBreaker::new("http-api", config),
        }
    }
    
    async fn get(&self, url: &str) -> Result<String, Error> {
        if !self.breaker.allow() {
            return Err(Error::CircuitOpen);
        }
        
        match reqwest::get(url).await {
            Ok(response) => {
                self.breaker.record_success();
                Ok(response.text().await?)
            }
            Err(e) => {
                self.breaker.record_failure();
                Err(e.into())
            }
        }
    }
}
```

### Proteger conexiones a base de datos

```rust
struct DbPool {
    breaker: CircuitBreaker,
    pool: Pool,
}

impl DbPool {
    async fn query(&self, sql: &str) -> Result<Rows, Error> {
        if !self.breaker.allow() {
            return Err(Error::DatabaseUnavailable);
        }
        
        match self.pool.get_connection().await {
            Ok(conn) => {
                match conn.query(sql).await {
                    Ok(rows) => {
                        self.breaker.record_success();
                        Ok(rows)
                    }
                    Err(e) => {
                        self.breaker.record_failure();
                        Err(e)
                    }
                }
            }
            Err(e) => {
                self.breaker.record_failure();
                Err(e)
            }
        }
    }
}
```

### Proteger operaciones de Kafka

```rust
// Ya implementado en kafka/consumer.rs y kafka/producer.rs
let config = Config::new(5, Duration::from_secs(30));
let breaker = CircuitBreaker::new("kafka-consumer", config);

if breaker.allow() {
    match consumer.poll(timeout) {
        Ok(msg) => breaker.record_success(),
        Err(_) => breaker.record_failure(),
    }
}
```

## ⚙️ Configuración Recomendada

### Para APIs HTTP

```rust
// APIs rápidas
Config::new(3, Duration::from_secs(10))
    .with_success_threshold(2)

// APIs lentas o inestables
Config::new(5, Duration::from_secs(60))
    .with_success_threshold(3)
```

### Para Bases de Datos

```rust
Config::new(10, Duration::from_secs(30))
    .with_success_threshold(5)
```

### Para Message Brokers (Kafka, RabbitMQ)

```rust
Config::new(5, Duration::from_secs(30))
    .with_success_threshold(2)
```

## 🧪 Testing

El módulo incluye tests exhaustivos:

```bash
cargo test circuit_breaker
```

Tests incluidos:
- ✅ Estado inicial CLOSED
- ✅ Transición CLOSED → OPEN tras threshold
- ✅ Transición OPEN → HALF_OPEN tras timeout
- ✅ Transición HALF_OPEN → CLOSED tras éxitos
- ✅ Transición HALF_OPEN → OPEN tras fallo
- ✅ Reset manual
- ✅ Success en CLOSED resetea contador
- ✅ Display de estados

## 📊 Comportamiento Detallado

### Estado CLOSED (Normal)

- ✅ `allow()` retorna `true`
- ✅ `record_success()` resetea contador de fallos
- ✅ `record_failure()` incrementa contador
- ⚠️ Si fallos ≥ threshold → transición a OPEN

### Estado OPEN (Falla Detectada)

- ❌ `allow()` retorna `false`
- ⏱️ Después de `reset_timeout` → transición a HALF_OPEN
- 🔄 `record_failure()` reinicia el timeout

### Estado HALF_OPEN (Probando Recuperación)

- ✅ `allow()` retorna `true` (permitir probar)
- ✅ `record_success()` incrementa contador de éxitos
- ✅ Si éxitos ≥ threshold → transición a CLOSED
- ❌ `record_failure()` → transición inmediata a OPEN

## 🎓 Principios de Diseño

1. **Single Responsibility**: Solo maneja lógica de circuit breaker
2. **Open/Closed**: Abierto a extensión, cerrado a modificación
3. **Dependency Inversion**: No depende de detalles de infraestructura
4. **Interface Segregation**: API mínima y clara
5. **KISS**: Simple y fácil de entender

## 🔐 Thread Safety

El circuit breaker es completamente thread-safe:

```rust
let breaker = Arc::new(CircuitBreaker::new("api", config));

// Uso desde múltiples threads
let breaker_clone = breaker.clone();
tokio::spawn(async move {
    if breaker_clone.allow() {
        // ...
    }
});
```

## ⚠️ Lo que NO hace

- ❌ No ejecuta operaciones (eso es responsabilidad del caller)
- ❌ No hace sleep/delay (solo verifica timeouts)
- ❌ No loggea (eso es responsabilidad de capas superiores)
- ❌ No conoce Kafka, HTTP, DB u otra tecnología específica

## 📚 Referencias

- [Martin Fowler - Circuit Breaker](https://martinfowler.com/bliki/CircuitBreaker.html)
- [Microsoft - Circuit Breaker Pattern](https://learn.microsoft.com/en-us/azure/architecture/patterns/circuit-breaker)
- [Release It! - Stability Patterns](https://pragprog.com/titles/mnee2/release-it-second-edition/)

---

**Versión**: 0.2.0  
**Mantenedor**: GeoContext Team  
**Licencia**: MIT
