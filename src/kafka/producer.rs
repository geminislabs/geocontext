use crate::circuit_breaker::CircuitBreaker;
use crate::config::KafkaConfig;
use anyhow::{Context, Result};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::ClientConfig;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Configuración para envío de un mensaje
pub struct ProduceRequest<'a> {
    pub topic: &'a str,
    pub key: Option<&'a [u8]>,
    pub payload: &'a [u8],
    pub headers: Option<Vec<(&'a str, &'a [u8])>>,
}

impl<'a> ProduceRequest<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8]) -> Self {
        Self {
            topic,
            key: None,
            payload,
            headers: None,
        }
    }

    pub fn with_key(mut self, key: &'a [u8]) -> Self {
        self.key = Some(key);
        self
    }

    pub fn with_string_key(self, key: &'a str) -> Self {
        self.with_key(key.as_bytes())
    }

    pub fn with_headers(mut self, headers: Vec<(&'a str, &'a [u8])>) -> Self {
        self.headers = Some(headers);
        self
    }
}

/// Respuesta de producción exitosa
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub partition: i32,
    pub offset: i64,
}

pub struct KafkaProducer {
    producer: FutureProducer,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig, circuit_breaker: Arc<CircuitBreaker>) -> Result<Self> {
        info!("Creating Kafka producer");

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("security.protocol", &config.security_protocol)
            .set("sasl.mechanism", &config.sasl_mechanism)
            .set("sasl.username", &config.username)
            .set("sasl.password", &config.password)
            .set("acks", &config.producer.acks)
            .set("linger.ms", &config.producer.linger_ms)
            .set("batch.size", &config.producer.batch_size)
            .set("retries", &config.producer.retries)
            .set("enable.idempotence", &config.producer.idempotent)
            .set("compression.type", "snappy")
            .set("max.in.flight.requests.per.connection", "5")
            .set("request.timeout.ms", "30000")
            .create()
            .context("Failed to create Kafka producer")?;

        info!(
            acks = %config.producer.acks,
            idempotent = %config.producer.idempotent,
            "Kafka producer initialized"
        );

        Ok(Self {
            producer,
            circuit_breaker,
        })
    }

    /// Envía un mensaje a Kafka usando ProduceRequest
    pub async fn send(&self, request: ProduceRequest<'_>) -> Result<ProduceResponse> {
        if !self.circuit_breaker.allow() {
            error!("Producer circuit breaker is OPEN, cannot send message");
            return Err(anyhow::anyhow!("Producer circuit breaker is OPEN"));
        }

        let mut record = FutureRecord::to(request.topic).payload(request.payload);

        if let Some(key) = request.key {
            record = record.key(key);
        }

        if let Some(headers) = request.headers {
            for (key, value) in headers {
                record = record.headers(
                    rdkafka::message::OwnedHeaders::new().insert(
                        rdkafka::message::Header {
                            key,
                            value: Some(value),
                        }
                    )
                );
            }
        }

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok((partition, offset)) => {
                debug!(
                    topic = %request.topic,
                    partition = partition,
                    offset = offset,
                    "Message sent successfully"
                );
                self.circuit_breaker.record_success();
                Ok(ProduceResponse { partition, offset })
            }
            Err((kafka_error, _)) => {
                error!(
                    error = ?kafka_error,
                    topic = %request.topic,
                    "Failed to send message to Kafka"
                );
                self.circuit_breaker.record_failure();
                Err(anyhow::anyhow!("Failed to produce message: {}", kafka_error))
            }
        }
    }

    pub async fn flush(&self) -> Result<()> {
        self.producer
            .flush(Duration::from_secs(10))
            .context("Failed to flush producer")?;
        
        debug!("Producer flushed");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_request_builder() {
        let payload = b"test payload";
        let request = ProduceRequest::new("test-topic", payload)
            .with_string_key("key-123");
        
        assert_eq!(request.topic, "test-topic");
        assert_eq!(request.key, Some(b"key-123" as &[u8]));
        assert_eq!(request.payload, payload);
    }
}
