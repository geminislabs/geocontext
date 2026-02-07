use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Evento de dominio: Mensaje mínimo de SISCOM
/// Esta es la representación de negocio, no de infraestructura
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiscomMinimalEvent {
    #[serde(flatten)]
    pub data: Value,
}

/// Evento de dominio: Mensaje enriquecido con contexto geográfico
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiscomEnrichedEvent {
    #[serde(flatten)]
    pub original: Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub geo_context: Option<GeoContext>,
}

/// Contexto geográfico agregado al evento
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub h3: Option<H3Context>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
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

impl SiscomMinimalEvent {
    pub fn new(data: Value) -> Self {
        Self { data }
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl SiscomEnrichedEvent {
    pub fn from_minimal(event: SiscomMinimalEvent) -> Self {
        Self {
            original: event.data,
            geo_context: None,
        }
    }

    pub fn with_geo_context(mut self, context: GeoContext) -> Self {
        self.geo_context = Some(context);
        self
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimal_event_creation() {
        let json = r#"{"id": 123, "lat": 19.4326, "lon": -99.1332}"#;
        let event = SiscomMinimalEvent::from_json(json).unwrap();
        assert!(event.data.get("id").is_some());
    }

    #[test]
    fn test_enriched_event_creation() {
        let json = r#"{"id": 456}"#;
        let minimal = SiscomMinimalEvent::from_json(json).unwrap();
        let enriched = SiscomEnrichedEvent::from_minimal(minimal);

        assert!(enriched.geo_context.is_none());
        assert_eq!(enriched.original.get("id").unwrap(), 456);
    }
}
