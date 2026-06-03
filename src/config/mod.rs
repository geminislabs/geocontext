use anyhow::{Context, Result};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub commit_on_produce_success: bool,
    pub health_bind_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    pub sasl_mechanism: String,
    pub username: String,
    pub password: String,
    pub security_protocol: String,
    pub input_topic_siscom: String,
    pub input_topic_mobility: String,
    pub output_topic_entity_position: String,
    pub consumer: ConsumerConfig,
    pub producer: ProducerConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConsumerConfig {
    pub fetch_min_bytes: String,
    pub fetch_wait_max_ms: String,
    pub max_poll_interval_ms: String,
    pub session_timeout_ms: String,
    pub enable_auto_commit: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProducerConfig {
    pub acks: String,
    pub linger_ms: String,
    pub batch_size: String,
    pub retries: String,
    pub idempotent: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: usize,
    pub reset_timeout_ms: u64,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let kafka = KafkaConfig {
            brokers: get_env("KAFKA_BROKERS")?,
            group_id: get_env("KAFKA_GROUP_ID")?,
            sasl_mechanism: get_env("KAFKA_SASL_MECHANISM")?,
            username: get_env("KAFKA_USERNAME")?,
            password: get_env("KAFKA_PASSWORD")?,
            security_protocol: get_env("KAFKA_SECURITY_PROTOCOL")?,
            input_topic_siscom: get_env("KAFKA_INPUT_TOPIC_SISCOM")?,
            input_topic_mobility: get_env("KAFKA_INPUT_TOPIC_MOBILITY")?,
            output_topic_entity_position: get_env("KAFKA_OUTPUT_TOPIC_ENTITY_POSITION")?,
            consumer: ConsumerConfig {
                fetch_min_bytes: get_env("KAFKA_CONSUMER_FETCH_MIN_BYTES")?,
                fetch_wait_max_ms: get_env("KAFKA_CONSUMER_FETCH_WAIT_MAX_MS")?,
                max_poll_interval_ms: get_env("KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS")?,
                session_timeout_ms: get_env("KAFKA_CONSUMER_SESSION_TIMEOUT_MS")?,
                enable_auto_commit: get_env("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT")?,
            },
            producer: ProducerConfig {
                acks: get_env("KAFKA_PRODUCER_ACKS")?,
                linger_ms: get_env("KAFKA_PRODUCER_LINGER_MS")?,
                batch_size: get_env("KAFKA_PRODUCER_BATCH_SIZE")?,
                retries: get_env("KAFKA_PRODUCER_RETRIES")?,
                idempotent: get_env("KAFKA_PRODUCER_IDEMPOTENT")?,
            },
        };

        let circuit_breaker = CircuitBreakerConfig {
            failure_threshold: get_env("CB_FAILURE_THRESHOLD")?
                .parse()
                .context("Invalid CB_FAILURE_THRESHOLD")?,
            reset_timeout_ms: get_env("CB_RESET_TIMEOUT_MS")?
                .parse()
                .context("Invalid CB_RESET_TIMEOUT_MS")?,
        };

        let commit_on_produce_success = get_env("COMMIT_ON_PRODUCE_SUCCESS")?
            .parse()
            .context("Invalid COMMIT_ON_PRODUCE_SUCCESS")?;

        let health_bind_addr = get_env_or_default("HEALTH_BIND_ADDR", "0.0.0.0:8080");

        Ok(Self {
            kafka,
            circuit_breaker,
            commit_on_produce_success,
            health_bind_addr,
        })
    }

    pub fn circuit_breaker_duration(&self) -> Duration {
        Duration::from_millis(self.circuit_breaker.reset_timeout_ms)
    }
}

impl KafkaConfig {
    pub fn input_topics(&self) -> Vec<&str> {
        vec![
            self.input_topic_siscom.as_str(),
            self.input_topic_mobility.as_str(),
        ]
    }
}

fn get_env(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("Missing environment variable: {}", key))
}

fn get_env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
