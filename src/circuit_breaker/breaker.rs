use crate::circuit_breaker::state::{InternalState, State};
use parking_lot::RwLock;
use std::time::{Duration, Instant};

/// Configuración del circuit breaker
///
/// Define los umbrales y timeouts que controlan el comportamiento del breaker.
#[derive(Debug, Clone)]
pub struct Config {
    /// Número de fallos consecutivos antes de abrir el circuito
    pub failure_threshold: usize,

    /// Tiempo de espera antes de intentar cerrar el circuito desde estado OPEN
    pub reset_timeout: Duration,

    /// Número de éxitos necesarios en HALF_OPEN para cerrar el circuito
    /// Por defecto: 2
    pub success_threshold_in_half_open: usize,
}

impl Config {
    /// Crea una configuración con valores por defecto razonables
    pub fn new(failure_threshold: usize, reset_timeout: Duration) -> Self {
        Self {
            failure_threshold,
            reset_timeout,
            success_threshold_in_half_open: 2,
        }
    }

    /// Configura el número de éxitos necesarios en estado HALF_OPEN
    #[allow(dead_code)]
    pub fn with_success_threshold(mut self, threshold: usize) -> Self {
        self.success_threshold_in_half_open = threshold;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold_in_half_open: 2,
        }
    }
}

/// Circuit Breaker genérico y reutilizable
///
/// Implementa el patrón Circuit Breaker para proteger sistemas de fallos en cascada.
/// Es completamente agnóstico al tipo de operación que protege (HTTP, Kafka, DB, etc.)
///
/// # Ejemplo
///
/// ```rust
/// use std::time::Duration;
/// use geocontext::circuit_breaker::{CircuitBreaker, Config};
///
/// let config = Config::new(3, Duration::from_secs(10));
/// let breaker = CircuitBreaker::new("my-service", config);
///
/// if breaker.allow() {
///     match external_call() {
///         Ok(_) => breaker.record_success(),
///         Err(_) => breaker.record_failure(),
///     }
/// }
/// ```
pub struct CircuitBreaker {
    config: Config,
    state: RwLock<InternalState>,
}

impl CircuitBreaker {
    /// Crea un nuevo circuit breaker con el nombre y configuración dados
    pub fn new(_name: impl Into<String>, config: Config) -> Self {
        Self {
            config,
            state: RwLock::new(InternalState::new()),
        }
    }

    /// Verifica si se permite ejecutar una operación
    ///
    /// Retorna `true` si el circuito está CLOSED o HALF_OPEN.
    /// Retorna `false` si el circuito está OPEN.
    ///
    /// Este método también maneja la transición automática de OPEN → HALF_OPEN
    /// cuando el reset_timeout ha expirado.
    pub fn allow(&self) -> bool {
        let mut state = self.state.write();

        match state.state {
            State::Closed | State::HalfOpen => true,
            State::Open => {
                // Verificar si es momento de transicionar a HALF_OPEN
                if let Some(last_failure) = state.last_failure_time {
                    if last_failure.elapsed() >= self.config.reset_timeout {
                        self.transition_to_half_open(&mut state);
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Registra una ejecución exitosa
    ///
    /// En estado CLOSED: resetea el contador de fallos
    /// En estado HALF_OPEN: incrementa éxitos y puede cerrar el circuito
    /// En estado OPEN: no tiene efecto
    pub fn record_success(&self) {
        let mut state = self.state.write();

        match state.state {
            State::Closed => {
                // Resetear contador de fallos
                state.failure_count = 0;
            }
            State::HalfOpen => {
                state.success_count_in_half_open += 1;

                // Si alcanzamos el umbral de éxitos, cerrar el circuito
                if state.success_count_in_half_open >= self.config.success_threshold_in_half_open {
                    self.transition_to_closed(&mut state);
                }
            }
            State::Open => {
                // No hacer nada en estado OPEN
            }
        }
    }

    /// Registra una ejecución fallida
    ///
    /// En estado CLOSED: incrementa contador y puede abrir el circuito
    /// En estado HALF_OPEN: reabre el circuito inmediatamente
    /// En estado OPEN: actualiza el timestamp de último fallo
    pub fn record_failure(&self) {
        let mut state = self.state.write();
        let now = Instant::now();

        match state.state {
            State::Closed => {
                state.failure_count += 1;
                state.last_failure_time = Some(now);

                // Verificar si debemos abrir el circuito
                if state.failure_count >= self.config.failure_threshold {
                    self.transition_to_open(&mut state);
                }
            }
            State::HalfOpen => {
                // Cualquier fallo en HALF_OPEN reabre el circuito
                state.last_failure_time = Some(now);
                self.transition_to_open(&mut state);
            }
            State::Open => {
                // Actualizar timestamp para reiniciar el timeout
                state.last_failure_time = Some(now);
            }
        }
    }

    /// Obtiene el estado actual del circuit breaker
    #[allow(dead_code)]
    pub fn state(&self) -> State {
        self.state.read().state
    }

    /// Resetea manualmente el circuit breaker a estado CLOSED
    ///
    /// Útil para testing o intervención manual en producción.
    #[allow(dead_code)]
    pub fn reset(&self) {
        let mut state = self.state.write();
        state.reset();
    }

    // Transiciones de estado internas

    fn transition_to_open(&self, state: &mut InternalState) {
        state.state = State::Open;
    }

    fn transition_to_half_open(&self, state: &mut InternalState) {
        state.state = State::HalfOpen;
        state.success_count_in_half_open = 0;
    }

    fn transition_to_closed(&self, state: &mut InternalState) {
        state.state = State::Closed;
        state.failure_count = 0;
        state.last_failure_time = None;
        state.success_count_in_half_open = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let config = Config::default();
        let breaker = CircuitBreaker::new("test", config);

        assert_eq!(breaker.state(), State::Closed);
        assert!(breaker.allow());
    }

    #[test]
    fn test_circuit_opens_after_threshold_failures() {
        let config = Config::new(3, Duration::from_secs(10));
        let breaker = CircuitBreaker::new("test", config);

        // Primera falla
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Closed);
        assert!(breaker.allow());

        // Segunda falla
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Closed);
        assert!(breaker.allow());

        // Tercera falla - debe abrir el circuito
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Open);
        assert!(!breaker.allow());
    }

    #[test]
    fn test_circuit_transitions_to_half_open_after_timeout() {
        let config = Config::new(2, Duration::from_millis(100));
        let breaker = CircuitBreaker::new("test", config);

        // Abrir el circuito
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Open);

        // Esperar el timeout
        sleep(Duration::from_millis(150));

        // Verificar transición a HALF_OPEN
        assert!(breaker.allow());
        assert_eq!(breaker.state(), State::HalfOpen);
    }

    #[test]
    fn test_half_open_closes_after_successes() {
        let config = Config::new(2, Duration::from_millis(50)).with_success_threshold(2);
        let breaker = CircuitBreaker::new("test", config);

        // Abrir y transicionar a HALF_OPEN
        breaker.record_failure();
        breaker.record_failure();
        sleep(Duration::from_millis(60));
        breaker.allow();

        assert_eq!(breaker.state(), State::HalfOpen);

        // Primera success
        breaker.record_success();
        assert_eq!(breaker.state(), State::HalfOpen);

        // Segunda success - debe cerrar
        breaker.record_success();
        assert_eq!(breaker.state(), State::Closed);
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let config = Config::new(2, Duration::from_millis(50));
        let breaker = CircuitBreaker::new("test", config);

        // Abrir y transicionar a HALF_OPEN
        breaker.record_failure();
        breaker.record_failure();
        sleep(Duration::from_millis(60));
        breaker.allow();

        assert_eq!(breaker.state(), State::HalfOpen);

        // Un fallo en HALF_OPEN debe reabrir
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Open);
    }

    #[test]
    fn test_success_in_closed_resets_failure_count() {
        let config = Config::new(3, Duration::from_secs(10));
        let breaker = CircuitBreaker::new("test", config);

        // Registrar algunas fallas
        breaker.record_failure();
        breaker.record_failure();

        // Un éxito debe resetear el contador
        breaker.record_success();

        // Ahora deberían necesitarse 3 fallos más para abrir
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), State::Open);
    }

    #[test]
    fn test_manual_reset() {
        let config = Config::new(2, Duration::from_secs(10));
        let breaker = CircuitBreaker::new("test", config);

        // Abrir el circuito
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), State::Open);

        // Reset manual
        breaker.reset();
        assert_eq!(breaker.state(), State::Closed);
        assert!(breaker.allow());
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new(10, Duration::from_secs(60)).with_success_threshold(5);

        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.reset_timeout, Duration::from_secs(60));
        assert_eq!(config.success_threshold_in_half_open, 5);
    }
}
