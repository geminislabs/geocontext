# Ejemplos de Mensajes

Este directorio contiene ejemplos de mensajes de entrada y salida para el microservicio GeoContext.

## Mensaje de Entrada (siscom-minimal)

### Ejemplo 1: Mensaje real (producción)
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
  "uuid": "ce69b8ac-4c55-5db8-a8b2-5b739b6b078e"
}
```

**Salida esperada** (siscom-geocontext):
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

---

### Ejemplo 2: Guadalajara (con números)
```json
{
  "id": 67890,
  "latitude": 20.6597,
  "longitude": -103.3496,
  "timestamp": "2024-01-15T11:00:00Z"
}
```

**Salida esperada**:
Mismo mensaje de entrada con `geo_context.h3` y campos `r10` → `r6` derivados jerárquicamente desde `r10`.

---

### Ejemplo 3: Monterrey (campos alternativos)
```json
{
  "id": 11111,
  "lat": "25.6866",
  "lon": "-100.3161",
  "event_time": "2024-01-15T12:00:00Z"
}
```

**Salida esperada**:
Mismo mensaje de entrada con `geo_context.h3` y campos `r10` → `r6` derivados jerárquicamente desde `r10`.

---

### Ejemplo 4: Sin coordenadas (passthrough)
```json
{
  "id": 22222,
  "event_type": "status_update",
  "status": "active",
  "timestamp": "2024-01-15T13:00:00Z"
}
```

**Salida esperada** (sin geo_context):
```json
{
  "id": 22222,
  "event_type": "status_update",
  "status": "active",
  "timestamp": "2024-01-15T13:00:00Z"
}
```

---

### Ejemplo 5: Coordenadas inválidas (passthrough con warning)
```json
{
  "id": 33333,
  "latitude": "999.0",
  "longitude": "-99.1332",
  "timestamp": "2024-01-15T14:00:00Z"
}
```

**Salida esperada** (sin geo_context, se loggea warning):
```json
{
  "id": 33333,
  "latitude": "999.0",
  "longitude": "-99.1332",
  "timestamp": "2024-01-15T14:00:00Z"
}
```

**Log esperado**:
```
WARN Failed to calculate H3 index for coordinates lat=999.0 lon=-99.1332
```

---

## Producir mensajes de prueba con kafka-console-producer

```bash
# Conectar al broker
kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic siscom-minimal \
  --property "parse.key=true" \
  --property "key.separator=:"

# Luego pegar cada línea (key:value)
12345:{"id":12345,"latitude":"19.4326","longitude":"-99.1332","timestamp":"2024-01-15T10:30:00Z"}
67890:{"id":67890,"latitude":20.6597,"longitude":-103.3496,"timestamp":"2024-01-15T11:00:00Z"}
11111:{"id":11111,"lat":"25.6866","lon":"-100.3161","event_time":"2024-01-15T12:00:00Z"}
```

## Consumir mensajes enriquecidos

```bash
# Consumir del topic de salida
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic siscom-geocontext \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" => "
```

---

## Notas

1. **Campos flexibles**: El pipeline soporta múltiples nombres de campos para coordenadas
2. **Tipos mixtos**: Coordenadas como string o número son parseadas correctamente
3. **Preservación**: TODOS los campos originales se preservan en el output
4. **Sin coordenadas**: Eventos sin coordenadas pasan sin modificación
5. **Validación**: Coordenadas inválidas resultan en passthrough con log de warning

## Resoluciones H3 comunes

| Resolución | Área aprox. | Uso típico |
|------------|-------------|------------|
| 8 | ~0.74 km² | Ciudad/Municipio |
| 9 | ~0.10 km² | Zona urbana |
| **10** | **~0.015 km²** | **Barrio/Colonia** ← Default |
| 11 | ~0.002 km² | Manzana grande |
| 12 | ~0.0003 km² | Manzana/Cuadra |
| 13 | ~0.00004 km² | Edificio |

El servicio genera `r10` desde lat/lon y deriva `r9` → `r6` por jerarquía. Actualmente `r10` es fijo a resolución 10.
