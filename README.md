# GeoContext Microservice

Microservicio de alto rendimiento en Rust para enriquecimiento geoespacial de mensajes Kafka usando índices H3.

## 🚀 Características

- **Enriquecimiento H3**: Calcula índices geoespaciales H3 a partir de coordenadas lat/lon
- **Alto rendimiento**: Optimizado para throughput máximo con configuración tuneada de Kafka
- **Arquitectura hexagonal**: Separación clara entre infraestructura, adaptadores y dominio
- **Resiliencia**: Circuit breakers genéricos con recuperación automática
- **Commit configurable**: Control granular del commit de offsets basado en éxito de producción
- **Procesamiento asíncrono**: Arquitectura completamente async con Tokio
- **Observabilidad**: Logging estructurado con tracing

## 📋 Requisitos

### Opción 1: DevContainer (Recomendado) 🐳
- Docker Desktop
- VS Code con extensión "Dev Containers"
- Red Docker `siscom-network`

### Opción 2: Instalación Local
- Rust 1.70+
- Kafka/Redpanda cluster
- librdkafka (instalado automáticamente por rdkafka-sys)

## 🛠️ Instalación

### Usando DevContainer (Recomendado)

```bash
# 1. Crear red Docker para Kafka
docker network create siscom-network

# 2. Abrir en VS Code
code .

# 3. Reabrir en contenedor
# F1 -> "Dev Containers: Reopen in Container"

# El contenedor:
# ✅ Instala Rust automáticamente
# ✅ Compila el proyecto
# ✅ Carga variables de .env
# ✅ Se conecta a siscom-network
```

Ver [.devcontainer/README.md](.devcontainer/README.md) para más detalles.

### Instalación Local

```bash
# Clonar el repositorio
git clone <repo-url>
cd GeoContext

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# Compilar
cargo build --release
```

## 🚀 Inicio Rápido

### Con DevContainer
```bash
# Dentro del contenedor
cargo run --release

# O usando Makefile
make run
```

### Local
```bash
# Cargar variables de entorno
source .env

# Ejecutar
cargo run --release
```

## ⚙️ Configuración

### Variables de Entorno Clave

#### Kafka Broker
- `KAFKA_BROKERS`: Lista de brokers (ej: `localhost:9092`)
- `KAFKA_GROUP_ID`: ID del consumer group
- `KAFKA_USERNAME` / `KAFKA_PASSWORD`: Credenciales SASL
- `KAFKA_INPUT_TOPIC`: Topic de entrada (`siscom-minimal`)
- `KAFKA_OUTPUT_TOPIC`: Topic de salida (`siscom-geocontext`)

#### Consumer Tuning
- `KAFKA_CONSUMER_FETCH_MIN_BYTES`: Bytes mínimos por fetch (default: 1024)
- `KAFKA_CONSUMER_FETCH_WAIT_MAX_MS`: Tiempo máximo de espera (default: 100ms)
- `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS`: Intervalo máximo entre polls (default: 300000ms)
- `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT`: **Debe ser `false`** para commit manual

#### Producer Tuning
- `KAFKA_PRODUCER_ACKS`: Nivel de ACKs (`all`, `1`, `0`)
- `KAFKA_PRODUCER_LINGER_MS`: Tiempo de batching (default: 5ms)
- `KAFKA_PRODUCER_BATCH_SIZE`: Tamaño del batch (default: 16384 bytes)
- `KAFKA_PRODUCER_RETRIES`: Número de reintentos (default: 5)
- `KAFKA_PRODUCER_IDEMPOTENT`: Idempotencia (`true`/`false`)

#### Commit Strategy
- `COMMIT_ON_PRODUCE_SUCCESS`: 
  - `true`: Commit solo si el mensaje se produjo exitosamente
  - `false`: Commit inmediato después del procesamiento

#### Circuit Breaker
- `CB_FAILURE_THRESHOLD`: Número de fallos antes de abrir el circuito (default: 5)
- `CB_RESET_TIMEOUT_MS`: Tiempo antes de intentar cerrar el circuito (default: 30000ms)

#### H3 Geospatial
- `H3_RESOLUTION`: Legacy (se conserva por compatibilidad, no afecta el cálculo actual)
- El servicio genera `r10` (resolución 10) y deriva `r9` → `r6` usando la jerarquía H3
  - **10**: Barrio/colonia (~0.015 km²) ← Base fija (r10)
  - **9**: Zona urbana (~0.10 km²)
  - **8**: Ciudad/municipio (~0.74 km²)
  - **7**: Región (~5.1 km²)
  - **6**: Región amplia (~36 km²)

## 🏃 Ejecución

```bash
# Modo desarrollo
cargo run

# Modo producción
cargo build --release
./target/release/geocontext
```

## 📊 Arquitectura

### Hexagonal Architecture (Ports & Adapters)

```
┌──────────────────────────────────────────────────────────────┐
│                    INFRASTRUCTURE LAYER                       │
│  ┌────────────────┐              ┌────────────────┐          │
│  │ KafkaConsumer  │◄─── CB ───   │ KafkaProducer  │◄─── CB   │
│  └────────┬───────┘              └────────┬───────┘          │
└───────────┼──────────────────────────────┼───────────────────┘
            │                              │
┌───────────┼──────────────────────────────┼───────────────────┐
│           │       ADAPTER LAYER          │                   │
│  ┌────────▼───────┐              ┌───────▼────────┐          │
│  │ InputConsumer  │              │ OutputProducer │          │
│  │ (Kafka → Msg)  │              │ (Msg → Kafka)  │          │
│  └────────┬───────┘              └───────▲────────┘          │
└───────────┼──────────────────────────────┼───────────────────┘
            │                              │
┌───────────┼──────────────────────────────┼───────────────────┐
│           │       APPLICATION LAYER      │                   │
│           │                              │                   │
│  ┌────────▼──────────────────────────────┴────────┐          │
│  │              Pipeline Processor                │          │
│  │          (Orchestration & Workflow)            │          │
│  └────────┬───────────────────────────────────────┘          │
│           │                                                  │
│  ┌────────▼───────┐                                          │
│  │  Enrichers     │                                          │
│  │  - H3 (lat/lon)│                                          │
│  └────────────────┘                                          │
└───────────────────────────────────────────────────────────────┘
            │
┌───────────▼───────────────────────────────────────────────────┐
│                      DOMAIN LAYER                             │
│  ┌─────────────────┐  ┌──────────────┐  ┌─────────────────┐  │
│  │ SiscomMinimal   │  │ GeoContext   │  │ SiscomEnriched  │  │
│  │ Event           │  │ - H3Context  │  │ Event           │  │
│  └─────────────────┘  └──────────────┘  └─────────────────┘  │
└───────────────────────────────────────────────────────────────┘
```

### Módulos

#### Infrastructure (`kafka/`)
- **KafkaConsumer**: Cliente Kafka puro, retorna bytes
- **KafkaProducer**: Cliente Kafka puro, envía bytes
- **Circuit Breaker**: Protección genérica para dependencias externas

#### Adapters
- **input/**: Convierte Kafka → Eventos de dominio
- **output/**: Convierte Eventos de dominio → Kafka

#### Application (`pipeline/`)
- **Processor**: Orquesta el flujo: input → enrich → output

#### Domain
- **event**: Modelos de negocio (`SiscomMinimalEvent`, `SiscomEnrichedEvent`, `GeoContext`, `H3Context`)
- **enrichers**: Lógica pura de enriquecimiento (sin efectos secundarios)
  - **h3**: Cálculo de índices H3 desde coordenadas

## 🔄 Flujo de Procesamiento

1. **Recepción**: `KafkaConsumer` recibe mensaje de `siscom-minimal` como bytes
2. **Circuit Breaker**: Verifica si el consumer está disponible
3. **Adaptación Input**: `InputConsumer` deserializa JSON a `SiscomMinimalEvent`
4. **Extracción**: Pipeline extrae coordenadas lat/lon del evento
5. **Enriquecimiento**: 
   - Valida coordenadas (rango, finitas)
   - Calcula índice H3 con resolución configurada
   - Construye `GeoContext` con `H3Context`
6. **Construcción**: Crea `SiscomEnrichedEvent` preservando campos originales
7. **Adaptación Output**: `OutputProducer` serializa a JSON
8. **Producción**: `KafkaProducer` envía mensaje enriquecido a `siscom-geocontext`
9. **Commit Condicional**:
   - Si `COMMIT_ON_PRODUCE_SUCCESS=true`: Commit solo si la producción fue exitosa
   - Si `COMMIT_ON_PRODUCE_SUCCESS=false`: Commit después del procesamiento

### Ejemplo de Transformación

**Input** (`siscom-minimal`):
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

**Output** (`siscom-geocontext`):
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

### Campos de Coordenadas Soportados

El pipeline busca automáticamente:
- **Latitud**: `latitude`, `lat`, `y`
- **Longitud**: `longitude`, `lon`, `lng`, `x`

Formatos: número directo o string parseable

## 🛡️ Manejo de Errores

### Circuit Breaker States

- **CLOSED**: Operación normal
- **OPEN**: Demasiados errores, consumo/producción pausado
- **HALF_OPEN**: Probando recuperación después del timeout

### Comportamiento ante Fallos

- Errores de Kafka incrementan contador de fallos
- Al superar `CB_FAILURE_THRESHOLD`, el circuito se abre
- Después de `CB_RESET_TIMEOUT_MS`, se intenta recuperación
- Logs claros de transiciones de estado

## 🧪 Testing

```bash
# Ejecutar todos los tests
cargo test

# Tests con output
cargo test -- --nocapture

# Tests específicos de módulo
cargo test circuit_breaker
cargo test enrichers::h3
cargo test pipeline

# Ver cobertura de tests
cargo test --all-features
```

### Usando Makefile

```bash
# Ver todos los comandos disponibles
make help

# Compilar y ejecutar
make build      # Compilar en release
make run        # Ejecutar
make test       # Tests
make check      # Formato + Lint + Tests

# Docker
make network    # Crear red siscom-network
make docker-up  # Levantar devcontainer
make dev        # Levantar y ver logs

# Desarrollo
make watch      # Auto-compilar en cambios
make fmt        # Formatear código
make lint       # Ejecutar clippy
```

Tests implementados (28 tests):
- ✅ Circuit breaker (8 tests)
- ✅ H3 enricher (10 tests)
- ✅ Domain events (2 tests)
- ✅ Kafka consumer/producer (2 tests)
- ✅ Input/Output adapters (2 tests)
- ✅ Pipeline processor (1 test)
- ✅ Models (3 tests)

## 📝 Logging

El sistema usa `tracing` para logging estructurado:

```bash
# Nivel de log configurable
RUST_LOG=debug cargo run
RUST_LOG=info,geocontext=trace cargo run
```

Logs incluyen:
- Inicialización de componentes
- Mensajes procesados (partition, offset)
- Transiciones de circuit breaker
- Errores con contexto completo

## 🔮 Próximas Mejoras

- [x] Implementar lógica de enriquecimiento con H3 ✅
- [x] Arquitectura hexagonal completa ✅
- [x] Circuit breaker genérico y reutilizable ✅
- [ ] Enriquecimiento con regiones geográficas (estados, municipios)
- [ ] Soporte para transacciones Kafka
- [ ] Métricas con Prometheus
- [ ] Health checks HTTP
- [ ] Retry policies configurables
- [ ] Dead letter queue para mensajes fallidos
- [ ] Enriquecimiento con datos de clima/tráfico

## 📚 Referencias

- [H3 Documentation](https://h3geo.org/)
- [h3o Crate](https://docs.rs/h3o/)
- [rdkafka](https://docs.rs/rdkafka/)
- [Hexagonal Architecture](https://alistair.cockburn.us/hexagonal-architecture/)

## 📄 Licencia

MIT
