use crate::domain::EntityPositionUpdate;
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
        event: &EntityPositionUpdate,
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

    #[test]
    fn test_entity_position_serialization() {
        let event = EntityPositionUpdate {
            source: Some("gps".to_string()),
            device_id: Some("vehicle_456".to_string()),
            lat: Some(20.5975),
            lon: Some(-100.3780),
            recorded_at: Some("2026-05-31T19:07:05Z".to_string()),
            received_at: Some("2026-05-31T19:35:31.150211Z".to_string()),
            accuracy_m: Some(3.0),
            speed_mps: Some(0.0),
            heading: Some(0.0),
            altitude_m: Some(1829.67),
            battery_level: Some(80.0),
            h3_10: Some("8a4983ca610ffff".to_string()),
            h3_10_ring_1: Some(vec![
                "8a4983ca6127fff".to_string(),
                "8a4983ca6107fff".to_string(),
            ]),
            h3_9: Some("894983ca610ffff".to_string()),
            h3_8: Some("884983ca61fffff".to_string()),
            h3_7: Some("874983ca6ffffff".to_string()),
        };

        let json_str = event.to_json().unwrap();
        assert!(json_str.contains("\"source\":\"gps\""));
        assert!(json_str.contains("\"h3_10\":\"8a4983ca610ffff\""));
    }
}
