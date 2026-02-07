use crate::domain::SiscomEnrichedEvent;
use crate::kafka::{KafkaProducer, ProduceRequest};
use anyhow::{Context, Result};
use tracing::{debug, info};

/// Adaptador: Dominio → Kafka
/// Convierte eventos de dominio enriquecidos a mensajes de Kafka
pub struct OutputProducer {
    kafka_producer: KafkaProducer,
    output_topic: String,
}

impl OutputProducer {
    pub fn new(kafka_producer: KafkaProducer, output_topic: String) -> Self {
        info!(topic = %output_topic, "Output producer initialized");
        Self {
            kafka_producer,
            output_topic,
        }
    }

    /// Publica un evento enriquecido al topic de salida
    pub async fn publish_event(
        &self,
        event: &SiscomEnrichedEvent,
        key: Option<&str>,
    ) -> Result<PublishResult> {
        let payload = event
            .to_json()
            .context("Failed to serialize enriched event to JSON")?;

        debug!(
            topic = %self.output_topic,
            payload_len = payload.len(),
            has_key = key.is_some(),
            "Publishing enriched event"
        );

        let mut request = ProduceRequest::new(&self.output_topic, payload.as_bytes());

        if let Some(k) = key {
            request = request.with_string_key(k);
        }

        let response = self
            .kafka_producer
            .send(request)
            .await
            .context("Failed to send message to Kafka")?;

        debug!(
            partition = response.partition,
            offset = response.offset,
            "Event published successfully"
        );

        Ok(PublishResult {
            partition: response.partition,
            offset: response.offset,
        })
    }
}

/// Resultado de la publicación de un evento
#[derive(Debug, Clone)]
pub struct PublishResult {
    pub partition: i32,
    pub offset: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_enriched_event_serialization() {
        use crate::domain::{GeoContext, H3Context, SiscomMinimalEvent};

        let minimal = SiscomMinimalEvent::new(json!({"id": 123}));
        let enriched = SiscomEnrichedEvent::from_minimal(minimal).with_geo_context(GeoContext {
            h3: Some(H3Context::new(
                "8c2a1072b59ffff".to_string(),
                "8b2a1072b59ffff".to_string(),
                "8a2a1072b59ffff".to_string(),
                "892a1072b59ffff".to_string(),
                "882a1072b59ffff".to_string(),
            )),
            region: None,
            metadata: None,
        });

        let json_str = enriched.to_json().unwrap();
        assert!(json_str.contains("\"h3\""));
        assert!(json_str.contains("8c2a1072b59ffff"));
    }
}
