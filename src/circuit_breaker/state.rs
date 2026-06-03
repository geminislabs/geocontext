use std::fmt;

/// Estados del circuit breaker según el patrón clásico
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Circuito cerrado: operaciones permitidas normalmente
    Closed,

    /// Circuito abierto: operaciones bloqueadas, sistema en recuperación
    Open,

    /// Circuito semi-abierto: probando si el sistema se recuperó
    HalfOpen,
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            State::Closed => write!(f, "CLOSED"),
            State::Open => write!(f, "OPEN"),
            State::HalfOpen => write!(f, "HALF_OPEN"),
        }
    }
}

/// Estado interno del circuit breaker
/// Mantiene contadores y timestamps necesarios para las transiciones
pub(crate) struct InternalState {
    pub state: State,
    pub failure_count: usize,
    pub last_failure_time: Option<std::time::Instant>,
    pub success_count_in_half_open: usize,
}

impl InternalState {
    pub fn new() -> Self {
        Self {
            state: State::Closed,
            failure_count: 0,
            last_failure_time: None,
            success_count_in_half_open: 0,
        }
    }

    /// Resetea completamente el estado interno
    pub fn reset(&mut self) {
        self.state = State::Closed;
        self.failure_count = 0;
        self.last_failure_time = None;
        self.success_count_in_half_open = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_display() {
        assert_eq!(State::Closed.to_string(), "CLOSED");
        assert_eq!(State::Open.to_string(), "OPEN");
        assert_eq!(State::HalfOpen.to_string(), "HALF_OPEN");
    }

    #[test]
    fn test_internal_state_new() {
        let state = InternalState::new();
        assert_eq!(state.state, State::Closed);
        assert_eq!(state.failure_count, 0);
        assert_eq!(state.success_count_in_half_open, 0);
        assert!(state.last_failure_time.is_none());
    }

    #[test]
    fn test_internal_state_reset() {
        let mut state = InternalState::new();
        state.state = State::Open;
        state.failure_count = 10;
        state.last_failure_time = Some(std::time::Instant::now());

        state.reset();

        assert_eq!(state.state, State::Closed);
        assert_eq!(state.failure_count, 0);
        assert!(state.last_failure_time.is_none());
    }
}
