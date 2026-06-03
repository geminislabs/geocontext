use crate::domain::H3Context;
use h3o::{CellIndex, LatLng, Resolution};
use std::str::FromStr;

/// Enriquece con índice H3 a partir de coordenadas lat/lon
///
/// Esta es una función pura que no tiene efectos secundarios:
/// - No loggea
/// - No conoce Kafka
/// - No accede a recursos externos
///
/// # Parámetros
/// - `lat`: Latitud en grados decimales (-90 a 90)
/// - `lon`: Longitud en grados decimales (-180 a 180)
///
/// # Retorna
/// - `Some(H3Context)` si las coordenadas son válidas
/// - `None` si las coordenadas son inválidas o la conversión falla
pub fn enrich_with_h3(lat: f64, lon: f64) -> Option<H3Context> {
    // Validar rangos de coordenadas
    if !is_valid_latitude(lat) || !is_valid_longitude(lon) {
        return None;
    }

    // Resolución base fija para r10
    let base_resolution = Resolution::try_from(10).ok()?;

    // Crear coordenadas H3
    let latlng = LatLng::new(lat, lon).ok()?;

    // Calcular índice H3 r10
    let cell_r10: CellIndex = latlng.to_cell(base_resolution);

    build_h3_context(cell_r10)
}

/// Calcula los vecinos ring-1 de una celda H3 en formato string.
/// Excluye la celda central del resultado.
pub fn h3_10_ring_1(h3_10: &str) -> Option<Vec<String>> {
    let center = CellIndex::from_str(h3_10).ok()?;
    let neighbors = center
        .grid_disk::<Vec<_>>(1)
        .into_iter()
        .filter(|index| *index != center)
        .map(|index| index.to_string())
        .collect::<Vec<_>>();

    Some(neighbors)
}

fn build_h3_context(cell_r10: CellIndex) -> Option<H3Context> {
    let r9 = cell_r10.parent(Resolution::try_from(9).ok()?)?;
    let r8 = cell_r10.parent(Resolution::try_from(8).ok()?)?;
    let r7 = cell_r10.parent(Resolution::try_from(7).ok()?)?;
    let r6 = cell_r10.parent(Resolution::try_from(6).ok()?)?;

    Some(H3Context::new(
        cell_r10.to_string(),
        r9.to_string(),
        r8.to_string(),
        r7.to_string(),
        r6.to_string(),
    ))
}

/// Valida que la latitud esté en el rango válido
#[inline]
fn is_valid_latitude(lat: f64) -> bool {
    (-90.0..=90.0).contains(&lat) && lat.is_finite()
}

/// Valida que la longitud esté en el rango válido
#[inline]
fn is_valid_longitude(lon: f64) -> bool {
    (-180.0..=180.0).contains(&lon) && lon.is_finite()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enrich_with_h3_valid_coordinates() {
        // Ciudad de México
        let result = enrich_with_h3(19.4326, -99.1332);
        assert!(result.is_some());

        let h3_ctx = result.unwrap();
        assert!(!h3_ctx.r10.is_empty());
        assert!(!h3_ctx.r9.is_empty());
        assert!(!h3_ctx.r8.is_empty());
        assert!(!h3_ctx.r7.is_empty());
        assert!(!h3_ctx.r6.is_empty());
        assert!(h3_ctx.r10.starts_with("8a"));
    }

    #[test]
    fn test_enrich_with_h3_different_resolutions() {
        let lat = 19.4326;
        let lon = -99.1332;

        let h3_ctx = enrich_with_h3(lat, lon).unwrap();
        assert_ne!(h3_ctx.r10, h3_ctx.r9);
        assert_ne!(h3_ctx.r9, h3_ctx.r8);
        assert_ne!(h3_ctx.r8, h3_ctx.r7);
        assert_ne!(h3_ctx.r7, h3_ctx.r6);
    }

    #[test]
    fn test_enrich_with_h3_invalid_latitude() {
        // Latitud fuera de rango
        assert!(enrich_with_h3(91.0, -99.1332).is_none());
        assert!(enrich_with_h3(-91.0, -99.1332).is_none());

        // Latitud infinita
        assert!(enrich_with_h3(f64::INFINITY, -99.1332).is_none());
        assert!(enrich_with_h3(f64::NEG_INFINITY, -99.1332).is_none());

        // NaN
        assert!(enrich_with_h3(f64::NAN, -99.1332).is_none());
    }

    #[test]
    fn test_enrich_with_h3_invalid_longitude() {
        // Longitud fuera de rango
        assert!(enrich_with_h3(19.4326, 181.0).is_none());
        assert!(enrich_with_h3(19.4326, -181.0).is_none());

        // Longitud infinita
        assert!(enrich_with_h3(19.4326, f64::INFINITY).is_none());
        assert!(enrich_with_h3(19.4326, f64::NEG_INFINITY).is_none());

        // NaN
        assert!(enrich_with_h3(19.4326, f64::NAN).is_none());
    }

    #[test]
    fn test_enrich_with_h3_edge_cases() {
        // Coordenadas en los límites
        assert!(enrich_with_h3(90.0, 180.0).is_some());
        assert!(enrich_with_h3(-90.0, -180.0).is_some());
        assert!(enrich_with_h3(0.0, 0.0).is_some());
    }

    #[test]
    fn test_enrich_with_h3_known_locations() {
        // Guadalajara, México
        let gdl = enrich_with_h3(20.6597, -103.3496);
        assert!(gdl.is_some());

        // Monterrey, México
        let mty = enrich_with_h3(25.6866, -100.3161);
        assert!(mty.is_some());

        // Los índices deben ser diferentes
        assert_ne!(gdl.unwrap().r10, mty.unwrap().r10);
    }

    #[test]
    fn test_is_valid_latitude() {
        assert!(is_valid_latitude(0.0));
        assert!(is_valid_latitude(45.0));
        assert!(is_valid_latitude(-45.0));
        assert!(is_valid_latitude(90.0));
        assert!(is_valid_latitude(-90.0));

        assert!(!is_valid_latitude(90.1));
        assert!(!is_valid_latitude(-90.1));
        assert!(!is_valid_latitude(f64::INFINITY));
        assert!(!is_valid_latitude(f64::NAN));
    }

    #[test]
    fn test_is_valid_longitude() {
        assert!(is_valid_longitude(0.0));
        assert!(is_valid_longitude(90.0));
        assert!(is_valid_longitude(-90.0));
        assert!(is_valid_longitude(180.0));
        assert!(is_valid_longitude(-180.0));

        assert!(!is_valid_longitude(180.1));
        assert!(!is_valid_longitude(-180.1));
        assert!(!is_valid_longitude(f64::INFINITY));
        assert!(!is_valid_longitude(f64::NAN));
    }

    #[test]
    fn test_h3_10_ring_1_neighbors() {
        let ring = h3_10_ring_1("8a4983ca610ffff").unwrap();
        assert!(!ring.is_empty());
        assert!(ring.iter().all(|idx| idx != "8a4983ca610ffff"));
    }
}
