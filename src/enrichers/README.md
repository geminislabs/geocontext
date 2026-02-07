# H3 Enricher

Enriquecedor geoespacial que calcula índices H3 a partir de coordenadas lat/lon.

## Descripción

Este módulo proporciona funcionalidad para enriquecer eventos con índices [H3](https://h3geo.org/), un sistema de indexación geoespacial jerárquica desarrollado por Uber.

## Características

- **Puro y sin efectos secundarios**: No accede a recursos externos ni produce logs
- **Validación robusta**: Verifica rangos válidos de coordenadas y maneja valores infinitos/NaN
- **Multi-resolución**: Genera `r10` y deriva `r9` → `r6` usando jerarquía H3
- **Alto rendimiento**: Utiliza la biblioteca `h3o` optimizada

## Uso

### Función Principal

```rust
use crate::enrichers::h3;

let lat = 19.4326;  // Ciudad de México
let lon = -99.1332;

// El parámetro `resolution` se conserva por compatibilidad, pero se ignora.
if let Some(h3_context) = h3::enrich_with_h3(lat, lon, 10) {
  println!("H3 r10: {}", h3_context.r10);
  println!("H3 r9:  {}", h3_context.r9);
  println!("H3 r8:  {}", h3_context.r8);
  println!("H3 r7:  {}", h3_context.r7);
  println!("H3 r6:  {}", h3_context.r6);
}
```

### Parámetros

- `lat`: Latitud en grados decimales (-90 a 90)
- `lon`: Longitud en grados decimales (-180 a 180)  
- `resolution`: Se mantiene por compatibilidad, pero **se ignora**. El enricher siempre calcula `r10` y deriva `r9` → `r6` desde la jerarquía H3.

### Retorno

- `Some(H3Context)`: Si las coordenadas son válidas
- `None`: Si las coordenadas son inválidas o la conversión falla

## Validaciones

El enricher valida:
- Latitud en rango [-90, 90]
- Longitud en rango [-180, 180]
- Valores finitos (rechaza Infinity y NaN)

## Integración con Pipeline

El pipeline extrae automáticamente coordenadas de los siguientes campos:

**Latitud**: `latitude`, `lat`, `y`  
**Longitud**: `longitude`, `lon`, `lng`, `x`

### Formatos soportados

```json
{
  "latitude": 19.4326,
  "longitude": -99.1332
}
```

```json
{
  "lat": "19.4326",
  "lon": "-99.1332"
}
```

Ambos formatos (número o string) son parseados correctamente.

## Configuración

La resolución base usada para `r10` es fija y actualmente es 10. Las resoluciones menores se derivan usando la jerarquía H3.

```bash
H3_RESOLUTION=10  # Legacy (no afecta el cálculo actual)
```

Valores de referencia:
- **10**: Barrio/colonia (~0.015 km²) ← Base fija (r10)
- **9**: Zona urbana (~0.10 km²)
- **8**: Ciudad/municipio (~0.74 km²)
- **7**: Región (~5.1 km²)
- **6**: Región amplia (~36 km²)

## Output

El resultado se agrega al campo `geo_context.h3` del evento enriquecido:

```json
{
  "backup_batery_voltage": "0.0",
  "cell_id": "03675103",
  "course": "0.00",
  "engine_status": "OFF",
  "fix_status": "1",
  "gps_datetime": "2024-04-09 16:22:26",
  "gps_epoch": 1712679746,
  "latitude": "+20.574605",
  "longitude": "-100.359826",
  "main_battery_voltage": "11.43",
  "mcc": "334",
  "mnc": "20",
  "msg_class": "STATUS",
  "network_status": "SERVER DISCONNECTED",
  "odometer": "730327",
  "received_at": 1770444644983,
  "rx_lvl": "33",
  "speed": "0.00",
  "stellites": "15",
  "uuid": "ce69b8ac-4c55-5db8-a8b2-5b739b6b078e",
  "geo_context": {
    "h3": {
      "r10": "8a4983d9b907fff",
      "r9": "894983d9b93ffff",
      "r8": "884983d9b9fffff",
      "r7": "874983d9bffffff",
      "r6": "864983d9fffffff"
    }
  }
}
```

## Testing

El módulo incluye tests exhaustivos:

```bash
cargo test enrichers::h3
```

Tests incluidos:
- Coordenadas válidas
- Diferentes resoluciones
- Latitudes inválidas (fuera de rango, infinitas, NaN)
- Longitudes inválidas
- Casos edge (límites de rangos)
- Ubicaciones conocidas (validación de consistencia)

## Referencias

- [H3 Documentation](https://h3geo.org/)
- [h3o Crate](https://docs.rs/h3o/)
- [H3 Resolution Table](https://h3geo.org/docs/core-library/restable)

## Arquitectura

```
Input Event (lat/lon)
        ↓
   enrich_with_h3()
        ↓
    Validate
        ↓
  LatLng::new()
        ↓
   to_cell()
        ↓
   H3Context
```

Este módulo es parte de la capa de **enrichers** en la arquitectura hexagonal, manteniendo completa independencia de infraestructura.
