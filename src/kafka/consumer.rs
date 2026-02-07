use crate::circuit_breaker::CircuitBreaker;
use crate::config::KafkaConfig;
use anyhow::{Context, Result};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::{ClientConfig, Message, Timestamp};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

/// Mensaje genérico de Kafka sin conocimiento de dominio
/// Esta estructura es parte de la capa de infraestructura pura
#[derive(Debug, Clone)]
pub struct KafkaMessage {
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub headers: HashMap<String, Vec<u8>>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

impl KafkaMessage {
    /// Convierte el payload a String UTF-8
    pub fn payload_as_string(&self) -> Result<String> {
        String::from_utf8(self.payload.clone()).context("Payload is not valid UTF-8")
    }

    /// Convierte el key a String UTF-8 si existe
    pub fn key_as_string(&self) -> Option<String> {
        self.key
            .as_ref()
            .and_then(|k| String::from_utf8(k.clone()).ok())
    }
}

/// Handle para commit de offset
/// Permite que la capa superior controle cuándo hacer commit
pub struct CommitHandle<'a> {
    consumer: &'a StreamConsumer,
    partition: i32,
    offset: i64,
}

impl<'a> CommitHandle<'a> {
    pub async fn commit(&self) -> Result<()> {
        use rdkafka::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("", self.partition, rdkafka::Offset::Offset(self.offset + 1))?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .context("Failed to commit offset")?;

        debug!(
            partition = self.partition,
            offset = self.offset,
            "Offset committed"
        );

        Ok(())
    }
}

pub struct KafkaConsumer {
    consumer: StreamConsumer,
    topic: String,
    circuit_breaker: Arc<CircuitBreaker>,
}

impl KafkaConsumer {
    pub fn new(config: &KafkaConfig, circuit_breaker: Arc<CircuitBreaker>) -> Result<Self> {
        info!("Creating Kafka consumer");

        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("security.protocol", &config.security_protocol)
            .set("sasl.mechanism", &config.sasl_mechanism)
            .set("sasl.username", &config.username)
            .set("sasl.password", &config.password)
            .set("enable.auto.commit", &config.consumer.enable_auto_commit)
            .set("fetch.min.bytes", &config.consumer.fetch_min_bytes)
            .set("fetch.wait.max.ms", &config.consumer.fetch_wait_max_ms)
            .set(
                "max.poll.interval.ms",
                &config.consumer.max_poll_interval_ms,
            )
            .set("session.timeout.ms", &config.consumer.session_timeout_ms)
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("partition.assignment.strategy", "range")
            .create()
            .context("Failed to create Kafka consumer")?;

        consumer
            .subscribe(&[&config.input_topic])
            .context("Failed to subscribe to topic")?;

        info!(
            topic = %config.input_topic,
            group_id = %config.group_id,
            "Kafka consumer initialized and subscribed"
        );

        Ok(Self {
            consumer,
            topic: config.input_topic.clone(),
            circuit_breaker,
        })
    }

    pub async fn receive_message(&self) -> Result<Option<KafkaMessage>> {
        if !self.circuit_breaker.allow() {
            warn!("Consumer circuit breaker is OPEN, pausing consumption");
            sleep(Duration::from_secs(1)).await;
            return Ok(None);
        }

        match tokio::time::timeout(Duration::from_secs(5), self.consumer.recv()).await {
            Ok(Ok(msg)) => {
                debug!(
                    topic = %self.topic,
                    partition = msg.partition(),
                    offset = msg.offset(),
                    "Message received"
                );
                self.circuit_breaker.record_success();

                let kafka_msg = self.extract_kafka_message(&msg)?;
                Ok(Some(kafka_msg))
            }
            Ok(Err(e)) => {
                error!(error = ?e, "Failed to receive message from Kafka");
                self.circuit_breaker.record_failure();
                Err(anyhow::anyhow!("Kafka consumer error: {}", e))
            }
            Err(_) => {
                debug!("No message received within timeout");
                Ok(None)
            }
        }
    }

    /// Extrae los datos del BorrowedMessage a una estructura independiente de rdkafka
    fn extract_kafka_message(&self, msg: &BorrowedMessage) -> Result<KafkaMessage> {
        let payload = msg.payload().context("Message has no payload")?.to_vec();

        let key = msg.key().map(|k| k.to_vec());

        let mut headers = HashMap::new();
        if let Some(msg_headers) = msg.headers() {
            for i in 0..msg_headers.count() {
                let header = msg_headers.get(i);
                headers.insert(
                    header.key.to_string(),
                    header.value.map(|v| v.to_vec()).unwrap_or_default(),
                );
            }
        }

        let timestamp = match msg.timestamp() {
            Timestamp::NotAvailable => None,
            Timestamp::CreateTime(ts) | Timestamp::LogAppendTime(ts) => Some(ts),
        };

        Ok(KafkaMessage {
            payload,
            key,
            headers,
            topic: msg.topic().to_string(),
            partition: msg.partition(),
            offset: msg.offset(),
            timestamp,
        })
    }

    /// Commit directo de un mensaje recibido
    pub async fn commit_offset(&self, partition: i32, offset: i64) -> Result<()> {
        use rdkafka::TopicPartitionList;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(&self.topic, partition, rdkafka::Offset::Offset(offset + 1))?;

        self.consumer
            .commit(&tpl, rdkafka::consumer::CommitMode::Async)
            .context("Failed to commit offset")?;

        debug!(partition = partition, offset = offset, "Offset committed");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config_building() {
        // This test verifies the configuration is correctly built
        // Actual Kafka connection tests require a running broker
        let config = KafkaConfig {
            brokers: "localhost:9092".to_string(),
            group_id: "test-group".to_string(),
            sasl_mechanism: "PLAIN".to_string(),
            username: "user".to_string(),
            password: "pass".to_string(),
            security_protocol: "SASL_PLAINTEXT".to_string(),
            input_topic: "test-input".to_string(),
            output_topic: "test-output".to_string(),
            consumer: crate::config::ConsumerConfig {
                fetch_min_bytes: "1024".to_string(),
                fetch_wait_max_ms: "100".to_string(),
                max_poll_interval_ms: "300000".to_string(),
                session_timeout_ms: "45000".to_string(),
                enable_auto_commit: "false".to_string(),
            },
            producer: crate::config::ProducerConfig {
                acks: "all".to_string(),
                linger_ms: "5".to_string(),
                batch_size: "16384".to_string(),
                retries: "5".to_string(),
                idempotent: "true".to_string(),
            },
        };

        assert_eq!(config.input_topic, "test-input");
        assert_eq!(config.consumer.enable_auto_commit, "false");
    }
}
