use crate::domain::SiscomMinimalEvent;
use crate::kafka::{KafkaConsumer, KafkaMessage};
use anyhow::{Context, Result};
use tracing::{debug, error};

/// Adaptador: Kafka → Dominio
/// Convierte mensajes de Kafka en eventos de dominio
pub struct InputConsumer {
    kafka_consumer: KafkaConsumer,
}

impl InputConsumer {
    pub fn new(kafka_consumer: KafkaConsumer) -> Self {
        Self { kafka_consumer }
    }

    /// Recibe un mensaje de Kafka y lo convierte a evento de dominio
    pub async fn receive_event(&self) -> Result<Option<(SiscomMinimalEvent, MessageContext)>> {
        let kafka_msg = match self.kafka_consumer.receive_message().await? {
            Some(msg) => msg,
            None => return Ok(None),
        };

        match self.parse_to_domain_event(&kafka_msg) {
            Ok(event) => {
                let context = MessageContext::from_kafka_message(&kafka_msg);
                Ok(Some((event, context)))
            }
            Err(e) => {
                error!(
                    error = ?e,
                    partition = kafka_msg.partition,
                    offset = kafka_msg.offset,
                    "Failed to parse Kafka message to domain event"
                );
                Err(e)
            }
        }
    }

    /// Parsea el mensaje de Kafka a evento de dominio
    fn parse_to_domain_event(&self, msg: &KafkaMessage) -> Result<SiscomMinimalEvent> {
        let payload_str = msg.payload_as_string()
            .context("Failed to convert payload to UTF-8")?;

        debug!(
            partition = msg.partition,
            offset = msg.offset,
            payload_len = payload_str.len(),
            "Parsing message to domain event"
        );

        SiscomMinimalEvent::from_json(&payload_str)
            .context("Failed to deserialize JSON to SiscomMinimalEvent")
    }

    /// Permite hacer commit del offset
    pub async fn commit_offset(&self, context: &MessageContext) -> Result<()> {
        self.kafka_consumer
            .commit_offset(context.partition, context.offset)
            .await
    }
}

/// Contexto del mensaje necesario para operaciones de infraestructura
/// Mantiene la información de Kafka sin exponer rdkafka fuera de la capa de adaptadores
#[derive(Debug, Clone)]
pub struct MessageContext {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<String>,
}

impl MessageContext {
    fn from_kafka_message(msg: &KafkaMessage) -> Self {
        Self {
            partition: msg.partition,
            offset: msg.offset,
            timestamp: msg.timestamp,
            key: msg.key_as_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_context_creation() {
        use std::collections::HashMap;
        
        let kafka_msg = KafkaMessage {
            payload: b"test".to_vec(),
            key: Some(b"key".to_vec()),
            headers: HashMap::new(),
            topic: "test".to_string(),
            partition: 0,
            offset: 123,
            timestamp: Some(1234567890),
        };

        let context = MessageContext::from_kafka_message(&kafka_msg);
        assert_eq!(context.partition, 0);
        assert_eq!(context.offset, 123);
        assert_eq!(context.key, Some("key".to_string()));
    }
}
