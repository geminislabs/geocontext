use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Evento de entrada genérico ya desacoplado de Kafka.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundEvent {
    pub topic: String,

    #[serde(flatten)]
    pub data: Value,
}

/// Mensaje canónico de salida para entity-position-updates.
/// Todos los campos son opcionales y se serializan como null cuando faltan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityPositionUpdate {
    pub source: Option<String>,
    pub device_id: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub recorded_at: Option<String>,
    pub received_at: Option<String>,
    pub accuracy_m: Option<f64>,
    pub speed_mps: Option<f64>,
    pub heading: Option<f64>,
    pub altitude_m: Option<f64>,
    pub battery_level: Option<f64>,
    pub h3_10: Option<String>,
    pub h3_10_ring_1: Option<Vec<String>>,
    pub h3_9: Option<String>,
    pub h3_8: Option<String>,
    pub h3_7: Option<String>,
}

/// Contexto H3 - Índice geoespacial
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct H3Context {
    pub r10: String,
    pub r9: String,
    pub r8: String,
    pub r7: String,
    pub r6: String,
}

impl H3Context {
    pub fn new(r10: String, r9: String, r8: String, r7: String, r6: String) -> Self {
        Self {
            r10,
            r9,
            r8,
            r7,
            r6,
        }
    }
}

impl InboundEvent {
    pub fn new(topic: impl Into<String>, data: Value) -> Self {
        Self {
            topic: topic.into(),
            data,
        }
    }
}

impl EntityPositionUpdate {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inbound_event_creation() {
        let event = InboundEvent::new("mobility-locations-raw", serde_json::json!({"id": 123}));
        assert_eq!(event.topic, "mobility-locations-raw");
        assert!(event.data.get("id").is_some());
    }

    #[test]
    fn test_entity_position_serialization_with_nulls() {
        let event = EntityPositionUpdate {
            source: Some("gps".to_string()),
            device_id: None,
            lat: None,
            lon: None,
            recorded_at: None,
            received_at: None,
            accuracy_m: None,
            speed_mps: None,
            heading: None,
            altitude_m: None,
            battery_level: None,
            h3_10: None,
            h3_10_ring_1: None,
            h3_9: None,
            h3_8: None,
            h3_7: None,
        };

        let json = event.to_json().unwrap();
        assert!(json.contains("\"source\":\"gps\""));
        assert!(json.contains("\"device_id\":null"));
    }
}
