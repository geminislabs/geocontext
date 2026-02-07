use crate::domain::{GeoContext, SiscomEnrichedEvent, SiscomMinimalEvent};
use crate::enrichers::h3;
use crate::input::{InputConsumer, MessageContext};
use crate::output::OutputProducer;
use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

/// Pipeline de procesamiento: orquesta el flujo input → enrich → output
/// Esta es la única capa que conoce tanto input como output
pub struct Processor {
    input: InputConsumer,
    output: OutputProducer,
    commit_on_produce_success: bool,
    h3_resolution: u8,
}

impl Processor {
    pub fn new(
        input: InputConsumer,
        output: OutputProducer,
        commit_on_produce_success: bool,
        h3_resolution: u8,
    ) -> Self {
        info!(
            commit_on_produce_success = commit_on_produce_success,
            h3_resolution = h3_resolution,
            "Pipeline processor initialized"
        );

        Self {
            input,
            output,
            commit_on_produce_success,
            h3_resolution,
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
            partition = context.partition,
            offset = context.offset,
            "Processing event"
        );

        // 2. Enriquecer el evento
        let enriched = self.enrich_event(event)?;

        // 3. Publicar al output adapter
        let publish_result = self
            .output
            .publish_event(&enriched, context.key.as_deref())
            .await
            .context("Failed to publish enriched event")?;

        debug!(
            input_partition = context.partition,
            input_offset = context.offset,
            output_partition = publish_result.partition,
            output_offset = publish_result.offset,
            "Event enriched and published"
        );

        // 4. Commit condicional basado en configuración
        if self.commit_on_produce_success {
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
            self.input
                .commit_offset(&context)
                .await
                .context("Failed to commit offset")?;
            
            debug!(
                partition = context.partition,
                offset = context.offset,
                "Offset committed immediately"
            );
        }

        Ok(true)
    }

    /// Lógica de enriquecimiento del evento
    /// Extrae coordenadas y aplica enriquecimientos geoespaciales
    fn enrich_event(&self, event: SiscomMinimalEvent) -> Result<SiscomEnrichedEvent> {
        debug!("Enriching event with geospatial data");
        
        // Intentar extraer coordenadas del evento
        let (lat, lon) = match self.extract_coordinates(&event) {
            Some(coords) => coords,
            None => {
                // Si no hay coordenadas, retornar evento sin enriquecimiento
                debug!("No coordinates found in event, skipping geo enrichment");
                return Ok(SiscomEnrichedEvent::from_minimal(event));
            }
        };

        // Aplicar enriquecimiento H3
        let h3_context = h3::enrich_with_h3(lat, lon, self.h3_resolution);

        if h3_context.is_none() {
            warn!(
                lat = lat,
                lon = lon,
                "Failed to calculate H3 index for coordinates"
            );
        }

        // Construir contexto geográfico
        let geo_context = GeoContext {
            h3: h3_context,
            region: None,
            metadata: None,
        };

        // Construir evento enriquecido
        let mut enriched = SiscomEnrichedEvent::from_minimal(event);
        enriched.geo_context = Some(geo_context);
        
        Ok(enriched)
    }

    /// Extrae coordenadas lat/lon del evento
    /// Busca campos comunes: latitude/longitude, lat/lon, etc.
    fn extract_coordinates(&self, event: &SiscomMinimalEvent) -> Option<(f64, f64)> {
        let data = &event.data;

        // Intentar diferentes variantes de nombres de campo
        let lat = self.extract_number(data, &["latitude", "lat", "y"])?;
        let lon = self.extract_number(data, &["longitude", "lon", "lng", "x"])?;

        Some((lat, lon))
    }

    /// Extrae un número de un campo del JSON
    /// Soporta tanto strings como números
    fn extract_number(&self, data: &serde_json::Value, field_names: &[&str]) -> Option<f64> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_enriched_event_transformation() {
        let event = SiscomMinimalEvent::new(json!({"id": 123}));
        let enriched = SiscomEnrichedEvent::from_minimal(event);
        
        assert!(enriched.geo_context.is_none());
        assert_eq!(enriched.original.get("id").unwrap(), 123);
    }
}

