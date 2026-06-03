use crate::domain::{EntityPositionUpdate, InboundEvent};
use crate::enrichers::h3;
use crate::input::InputConsumer;
use crate::output::OutputProducer;
use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

/// Pipeline de procesamiento: orquesta el flujo input → enrich → output
/// Esta es la única capa que conoce tanto input como output
pub struct Processor {
    input: InputConsumer,
    output: OutputProducer,
    commit_on_produce_success: bool,
    siscom_topic: String,
    mobility_topic: String,
}

impl Processor {
    pub fn new(
        input: InputConsumer,
        output: OutputProducer,
        commit_on_produce_success: bool,
        siscom_topic: String,
        mobility_topic: String,
    ) -> Self {
        info!(
            commit_on_produce_success = commit_on_produce_success,
            "Pipeline processor initialized"
        );

        Self {
            input,
            output,
            commit_on_produce_success,
            siscom_topic,
            mobility_topic,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting message processing pipeline");

        loop {
            match self.process_single_message().await {
                Ok(processed) => {
                    if !processed {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
                Err(e) => {
                    error!(error = ?e, "Error processing message");
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                }
            }
        }
    }

    async fn process_single_message(&self) -> Result<bool> {
        // 1. Recibir evento del input adapter
        let (event, context) = match self.input.receive_event().await? {
            Some(data) => data,
            None => return Ok(false),
        };

        debug!(
            topic = %context.topic,
            partition = context.partition,
            offset = context.offset,
            "Processing event"
        );

        // 2. Enriquecer el evento
        let enriched = self.enrich_event(event)?;
        let key = enriched.device_id.as_deref().or(context.key.as_deref());

        // 3. Publicar al output adapter
        let publish_result = self.output.publish_event(&enriched, key).await;

        // 4. Commit condicional basado en configuración
        if self.commit_on_produce_success {
            let publish_result = publish_result.context("Failed to publish enriched event")?;

            debug!(
                input_topic = %context.topic,
                input_partition = context.partition,
                input_offset = context.offset,
                output_partition = publish_result.partition,
                output_offset = publish_result.offset,
                "Event enriched and published"
            );

            self.input
                .commit_offset(&context)
                .await
                .context("Failed to commit offset after successful produce")?;

            debug!(
                partition = context.partition,
                offset = context.offset,
                "Offset committed after successful produce"
            );
        } else {
            match publish_result {
                Ok(result) => {
                    debug!(
                        input_topic = %context.topic,
                        input_partition = context.partition,
                        input_offset = context.offset,
                        output_partition = result.partition,
                        output_offset = result.offset,
                        "Event enriched and published"
                    );
                }
                Err(e) => {
                    warn!(
                        error = ?e,
                        partition = context.partition,
                        offset = context.offset,
                        "Publish failed but commit_on_produce_success is disabled; committing offset"
                    );
                }
            }

            self.input
                .commit_offset(&context)
                .await
                .context("Failed to commit offset after processing")?;

            debug!(
                partition = context.partition,
                offset = context.offset,
                "Offset committed after processing"
            );
        }

        Ok(true)
    }

    /// Lógica de enriquecimiento del evento
    /// Extrae coordenadas y aplica enriquecimientos geoespaciales
    fn enrich_event(&self, event: InboundEvent) -> Result<EntityPositionUpdate> {
        debug!(topic = %event.topic, "Normalizing input event to canonical entity position update");

        let mut canonical = Self::normalize_event(&event, &self.siscom_topic, &self.mobility_topic);

        if let (Some(lat), Some(lon)) = (canonical.lat, canonical.lon) {
            let h3_context = h3::enrich_with_h3(lat, lon);

            if let Some(h3_ctx) = h3_context {
                if canonical.h3_10.is_none() {
                    canonical.h3_10 = Some(h3_ctx.r10);
                }
                if canonical.h3_9.is_none() {
                    canonical.h3_9 = Some(h3_ctx.r9);
                }
                if canonical.h3_8.is_none() {
                    canonical.h3_8 = Some(h3_ctx.r8);
                }
                if canonical.h3_7.is_none() {
                    canonical.h3_7 = Some(h3_ctx.r7);
                }
            } else {
                warn!(
                    lat = lat,
                    lon = lon,
                    "Failed to calculate H3 index for coordinates"
                );
            }
        } else {
            debug!("No coordinates found in event, skipping H3 enrichment");
        }

        canonical.h3_10_ring_1 = canonical.h3_10.as_deref().and_then(h3::h3_10_ring_1);

        Ok(canonical)
    }

    fn normalize_event(
        event: &InboundEvent,
        siscom_topic: &str,
        mobility_topic: &str,
    ) -> EntityPositionUpdate {
        let data = &event.data;
        let is_siscom = event.topic == siscom_topic;
        let is_mobility = event.topic == mobility_topic;

        let source = if is_siscom {
            Some("gps".to_string())
        } else if is_mobility {
            Self::extract_string(data, &["source"]).or_else(|| Some("mobility".to_string()))
        } else {
            Self::extract_string(data, &["source"])
        };

        let mut h3_10 = Self::extract_string(data, &["h3_10"]);
        if h3_10.is_none() {
            let h3_index = Self::extract_string(data, &["h3_index"]);
            let h3_resolution = Self::extract_number(data, &["h3_resolution"]);
            if matches!(h3_resolution, Some(res) if (res - 10.0).abs() < f64::EPSILON) {
                h3_10 = h3_index;
            }
        }

        EntityPositionUpdate {
            source,
            device_id: Self::extract_string(data, &["device_id", "uuid"]),
            lat: Self::extract_number(data, &["lat", "latitude", "y"]),
            lon: Self::extract_number(data, &["lon", "longitude", "lng", "x"]),
            recorded_at: Self::extract_string(data, &["recorded_at", "gps_datetime"]),
            received_at: Self::extract_string(data, &["received_at"]),
            accuracy_m: Self::extract_number(data, &["accuracy_m"]),
            speed_mps: Self::extract_speed_mps(data, is_siscom),
            heading: Self::extract_number(data, &["heading", "course"]),
            altitude_m: Self::extract_number(data, &["altitude_m", "altitude"]),
            battery_level: Self::extract_number(data, &["battery_level"]),
            h3_10,
            h3_10_ring_1: Self::extract_string_array(data, &["h3_10_ring_1"]),
            h3_9: Self::extract_string(data, &["h3_9"]),
            h3_8: Self::extract_string(data, &["h3_8"]),
            h3_7: Self::extract_string(data, &["h3_7"]),
        }
    }

    /// Extrae velocidad en m/s.
    /// Para SISCOM convierte `speed` de km/h a m/s.
    fn extract_speed_mps(data: &serde_json::Value, is_siscom: bool) -> Option<f64> {
        if let Some(speed_mps) = Self::extract_number(data, &["speed_mps"]) {
            return Some(speed_mps);
        }

        let speed = Self::extract_number(data, &["speed"])?;
        if is_siscom {
            Some(speed / 3.6)
        } else {
            Some(speed)
        }
    }

    /// Extrae coordenadas lat/lon del evento
    /// Busca campos comunes: latitude/longitude, lat/lon, etc.
    /// Extrae un número de un campo del JSON.
    /// Soporta tanto strings como números.
    fn extract_number(data: &serde_json::Value, field_names: &[&str]) -> Option<f64> {
        for field in field_names {
            if let Some(value) = data.get(field) {
                // Intentar como número directo
                if let Some(num) = value.as_f64() {
                    return Some(num);
                }

                // Intentar parsear string
                if let Some(s) = value.as_str() {
                    if let Ok(num) = s.trim().parse::<f64>() {
                        return Some(num);
                    }
                }
            }
        }
        None
    }

    /// Extrae un string de un campo del JSON.
    /// Soporta strings y números convertibles a texto.
    fn extract_string(data: &serde_json::Value, field_names: &[&str]) -> Option<String> {
        for field in field_names {
            if let Some(value) = data.get(field) {
                if let Some(s) = value.as_str() {
                    return Some(s.to_string());
                }
                if value.is_number() || value.is_boolean() {
                    return Some(value.to_string());
                }
            }
        }
        None
    }

    /// Extrae un arreglo de strings de un campo JSON.
    fn extract_string_array(data: &serde_json::Value, field_names: &[&str]) -> Option<Vec<String>> {
        for field in field_names {
            if let Some(value) = data.get(field) {
                if let Some(array) = value.as_array() {
                    let values = array
                        .iter()
                        .filter_map(|item| item.as_str().map(ToString::to_string))
                        .collect::<Vec<_>>();
                    return Some(values);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_normalize_siscom_source_and_device_fallback() {
        let event = InboundEvent::new(
            "siscom-minimal",
            json!({
                "uuid": "vehicle_456",
                "latitude": 20.59,
                "longitude": -100.37
            }),
        );

        let normalized =
            Processor::normalize_event(&event, "siscom-minimal", "mobility-locations-raw");

        assert_eq!(normalized.source.as_deref(), Some("gps"));
        assert_eq!(normalized.device_id.as_deref(), Some("vehicle_456"));
        assert_eq!(normalized.lat, Some(20.59));
        assert_eq!(normalized.lon, Some(-100.37));
    }

    #[test]
    fn test_normalize_siscom_speed_converts_kmh_to_mps() {
        let event = InboundEvent::new(
            "siscom-minimal",
            json!({
                "speed": "36.0"
            }),
        );

        let normalized =
            Processor::normalize_event(&event, "siscom-minimal", "mobility-locations-raw");

        assert_eq!(normalized.speed_mps, Some(10.0));
    }

    #[test]
    fn test_normalize_mobility_speed_mps_is_preserved() {
        let event = InboundEvent::new(
            "mobility-locations-raw",
            json!({
                "speed_mps": 4.2,
                "speed": 100.0
            }),
        );

        let normalized =
            Processor::normalize_event(&event, "siscom-minimal", "mobility-locations-raw");

        assert_eq!(normalized.speed_mps, Some(4.2));
    }

    #[test]
    fn test_h3_10_ring_1_is_calculated_from_h3_10() {
        let mut canonical = EntityPositionUpdate {
            source: Some("mobility".to_string()),
            device_id: Some("vehicle_456".to_string()),
            lat: None,
            lon: None,
            recorded_at: None,
            received_at: None,
            accuracy_m: None,
            speed_mps: None,
            heading: None,
            altitude_m: None,
            battery_level: None,
            h3_10: Some("8a4983ca610ffff".to_string()),
            h3_10_ring_1: None,
            h3_9: None,
            h3_8: None,
            h3_7: None,
        };

        canonical.h3_10_ring_1 = canonical.h3_10.as_deref().and_then(h3::h3_10_ring_1);

        let ring = canonical.h3_10_ring_1.unwrap();
        assert!(!ring.is_empty());
        assert!(!ring.contains(&"8a4983ca610ffff".to_string()));
    }
}
