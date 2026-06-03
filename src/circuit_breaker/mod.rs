//! # Circuit Breaker Module
//!
//! Implementación genérica y reutilizable del patrón Circuit Breaker.
//!
//! Este módulo es completamente agnóstico al tipo de operación que protege.
//! Puede usarse para proteger llamadas a:
//! - Kafka
//! - APIs HTTP
//! - Bases de datos
//! - Cualquier dependencia externa
//!
//! ## Ejemplo de Uso
//!
//! ```rust
//! use std::time::Duration;
//! use geocontext::circuit_breaker::{CircuitBreaker, Config};
//!
//! let config = Config::new(3, Duration::from_secs(30));
//! let breaker = CircuitBreaker::new("external-api", config);
//!
//! // Antes de cada operación
//! if breaker.allow() {
//!     match perform_operation() {
//!         Ok(result) => {
//!             breaker.record_success();
//!             // usar result
//!         }
//!         Err(e) => {
//!             breaker.record_failure();
//!             // manejar error
//!         }
//!     }
//! } else {
//!     // Circuit está abierto, no ejecutar operación
//! }
//! # fn perform_operation() -> Result<(), ()> { Ok(()) }
//! ```

mod breaker;
mod state;

pub use breaker::{CircuitBreaker, Config};
