use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Representa el mensaje mínimo recibido del topic siscom-minimal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiscomMinimal {
    #[serde(flatten)]
    pub data: Value,
}

/// Representa el mensaje enriquecido que se envía al topic siscom-geocontext
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiscomGeoContext {
    #[serde(flatten)]
    pub original: Value,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrichment: Option<GeoEnrichment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoEnrichment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub h3_index: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

impl SiscomMinimal {
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }
}

impl SiscomGeoContext {
    pub fn from_minimal(minimal: SiscomMinimal) -> Self {
        Self {
            original: minimal.data,
            enrichment: None,
        }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self)
    }

    pub fn with_enrichment(mut self, enrichment: GeoEnrichment) -> Self {
        self.enrichment = Some(enrichment);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_siscom_minimal_parsing() {
        let json = r#"{"id": 123, "timestamp": "2024-01-01T00:00:00Z", "data": "test"}"#;
        let minimal = SiscomMinimal::from_json(json).unwrap();
        assert!(minimal.data.get("id").is_some());
    }

    #[test]
    fn test_siscom_geocontext_creation() {
        let json = r#"{"id": 123, "lat": 40.7128, "lon": -74.0060}"#;
        let minimal = SiscomMinimal::from_json(json).unwrap();
        let geo = SiscomGeoContext::from_minimal(minimal);
        
        assert!(geo.enrichment.is_none());
        assert!(geo.original.get("id").is_some());
    }
}
