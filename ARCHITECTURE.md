# Arquitectura Hexagonal - GeoContext

## 📐 Diseño de Capas

El proyecto `geocontext` implementa una **arquitectura hexagonal (ports & adapters)** para mantener una clara separación de responsabilidades:

```
┌─────────────────────────────────────────────────────────────────┐
│                         APLICACIÓN                              │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │            Pipeline (Orquestación)                      │  │
│  │  - Coordina input → enrich → output                     │  │
│  │  - Maneja commit condicional                            │  │
│  └─────────────────────────────────────────────────────────┘  │
│                           ▲                                     │
└───────────────────────────┼─────────────────────────────────────┘
                            │
         ┌──────────────────┴──────────────────┐
         │                                     │
         ▼                                     ▼
┌──────────────────┐                  ┌──────────────────┐
│  INPUT ADAPTER   │                  │ OUTPUT ADAPTER   │
│                  │                  │                  │
│ Kafka → Dominio  │                  │ Dominio → Kafka  │
│                  │                  │                  │
│ - Consume msgs   │                  │ - Serializa      │
│ - Parsea JSON    │                  │ - Produce msgs   │
│ - Valida         │                  │                  │
└────────┬─────────┘                  └─────────┬────────┘
         │                                      │
         │    DOMINIO (Eventos de Negocio)      │
         │    ┌───────────────────────────┐     │
         └───►│ InboundEvent              │◄────┘
              │ EntityPositionUpdate      │
              │ H3Context                 │
              └───────────────────────────┘
         │                                      │
         ▼                                      ▼
┌──────────────────┐                  ┌──────────────────┐
│ INFRAESTRUCTURA  │                  │ INFRAESTRUCTURA  │
│                  │                  │                  │
│ KafkaConsumer    │                  │ KafkaProducer    │
│ - rdkafka puro   │                  │ - rdkafka puro   │
│ - Circuit break  │                  │ - Circuit break  │
│ - Retorna bytes  │                  │ - Recibe bytes   │
└──────────────────┘                  └──────────────────┘
```

## 🔍 Separación de Responsabilidades

### 1. **Capa de Infraestructura** (`kafka/`)

**Responsabilidad**: Comunicación con Kafka usando `rdkafka`

#### `kafka/consumer.rs`
- ✅ Configuración de librdkafka
- ✅ Poll de mensajes
- ✅ Manejo de circuit breaker
- ✅ Retorna `KafkaMessage` (estructura genérica)
- ❌ **NO** conoce modelos de dominio
- ❌ **NO** parsea JSON de negocio

**Estructura clave**:
```rust
pub struct KafkaMessage {
    pub payload: Vec<u8>,        // Bytes sin procesar
    pub key: Option<Vec<u8>>,
    pub headers: HashMap<String, Vec<u8>>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}
```

#### `kafka/producer.rs`
- ✅ Configuración de librdkafka
- ✅ Envío de mensajes
- ✅ Manejo de circuit breaker
- ✅ Recibe `ProduceRequest` (genérico)
- ❌ **NO** conoce modelos de dominio
- ❌ **NO** serializa objetos de negocio

**Estructura clave**:
```rust
pub struct ProduceRequest<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],      // Bytes genéricos
    pub key: Option<&'a [u8]>,
    pub headers: Option<Vec<(&'a str, &'a [u8])>>,
}
```

---

### 2. **Capa de Dominio** (`domain/`)

**Responsabilidad**: Modelos de negocio puros

#### `domain/event.rs`
```rust
pub struct InboundEvent {
    pub topic: String,
    pub data: Value,
}

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
```

- ✅ Sin dependencias de infraestructura
- ✅ Lógica de negocio pura
- ✅ Serializable/Deserializable

---

### 3. **Capa de Adaptadores**

#### **Input Adapter** (`input/consumer.rs`)

**Responsabilidad**: Kafka → Dominio

```rust
pub struct InputConsumer {
    kafka_consumer: KafkaConsumer,  // Usa infraestructura
}

impl InputConsumer {
    pub async fn receive_event(&self) 
        -> Result<Option<(InboundEvent, MessageContext)>> 
    {
        // 1. Recibe KafkaMessage de la infra
        // 2. Convierte a InboundEvent (dominio)
        // 3. Retorna evento + contexto
    }
}
```

**Flujo**:
```
KafkaMessage (bytes) → JSON parsing → InboundEvent (dominio)
```

**`MessageContext`**: Mantiene información de Kafka necesaria para commit, pero sin exponer `rdkafka`:
```rust
pub struct MessageContext {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<String>,
}
```

#### **Output Adapter** (`output/producer.rs`)

**Responsabilidad**: Dominio → Kafka

```rust
pub struct OutputProducer {
    kafka_producer: KafkaProducer,  // Usa infraestructura
    output_topic: String,
}

impl OutputProducer {
    pub async fn publish_event(
        &self,
        event: &EntityPositionUpdate,  // Dominio
        key: Option<&str>,
    ) -> Result<PublishResult> 
    {
        // 1. Serializa evento a JSON
        // 2. Crea ProduceRequest con bytes
        // 3. Envía vía KafkaProducer
    }
}
```

**Flujo**:
```
EntityPositionUpdate (dominio) → JSON serialization → ProduceRequest (bytes) → Kafka
```

---

### 4. **Capa de Aplicación** (`pipeline/`)

**Responsabilidad**: Orquestación del flujo completo

#### `pipeline/processor.rs`

```rust
pub struct Processor {
    input: InputConsumer,     // Adaptador de entrada
    output: OutputProducer,   // Adaptador de salida
    commit_on_produce_success: bool,
}
```

**Flujo de procesamiento**:

1. **Recibir**: `input.receive_event()` → `(InboundEvent, MessageContext)`
2. **Normalizar + Enriquecer**: `enrich_event()` → `EntityPositionUpdate`
3. **Publicar**: `output.publish_event()` → `PublishResult`
4. **Commit condicional**:
   - Si `commit_on_produce_success=true`: commit solo si publish OK
   - Si `commit_on_produce_success=false`: commit inmediato

```rust
async fn process_single_message(&self) -> Result<bool> {
    // 1. Input adapter: Kafka → Dominio
    let (event, context) = self.input.receive_event().await?;
    
    // 2. Normalización + enriquecimiento
    let enriched = self.enrich_event(event)?;
    
    // 3. Output adapter: Dominio → Kafka
    self.output.publish_event(&enriched, context.key).await?;
    
    // 4. Commit condicional
    if self.commit_on_produce_success {
        self.input.commit_offset(&context).await?;
    }
}
```

---

## 🎯 Ventajas de esta Arquitectura

### 1. **Independencia de Kafka**
- La lógica de dominio **no conoce** rdkafka
- Se puede cambiar de Kafka a RabbitMQ/NATS sin tocar dominio
- Los adaptadores actúan como "puertos" intercambiables

### 2. **Testabilidad**
- Dominio es puro → tests unitarios sin mocks
- Adaptadores se pueden mockear fácilmente
- Infraestructura se puede testear aisladamente

### 3. **Claridad de Responsabilidades**
```
kafka/       → "¿Cómo me comunico con Kafka?"
input/       → "¿Cómo convierto mensajes Kafka a eventos?"
output/      → "¿Cómo convierto eventos a mensajes Kafka?"
domain/      → "¿Qué significa este evento de negocio?"
pipeline/    → "¿Cómo orquesto el flujo completo?"
```

### 4. **Evolución Independiente**
- Cambios en rdkafka → solo afecta `kafka/`
- Cambios en formato JSON → solo afecta adaptadores
- Nuevos enriquecimientos → solo afecta `pipeline/` y `enrichers/`

---

## 🧭 Vecinos H3 (`h3_10_ring_1`)

El mensaje canónico de salida incluye:

```json
{
    "h3_10": "A",
    "h3_10_ring_1": ["B", "C", "D", "E", "F", "G"]
}
```

Semántica esperada del campo:

```text
gridRing(h3_10, 1)
```

Diferencia operativa:

- `gridRing(h, 1)`: devuelve solo la corona de distancia exacta 1 (sin `h`).
- `gridDisk(h, 1)`: devuelve distancia <= 1 (incluye `h` + vecinos).

Decisión de este servicio:

- Se usa `gridDisk(h3_10, 1)` y se excluye la celda central.
- Motivo: normalizar vecinos mínimos sin repetir `h3_10`, manteniendo un formato canónico que otras entidades puedan explotar directamente.

---

## 🔄 Flujo Completo de un Mensaje

```
1. INFRAESTRUCTURA (kafka/consumer.rs)
    ┌─────────────────────────────────────┐
    │ KafkaConsumer::receive_message()    │
    │ ↓                                   │
    │ KafkaMessage { payload: Vec<u8> }   │
    └──────────────┬──────────────────────┘
                        │
2. INPUT ADAPTER (input/consumer.rs)
    ┌──────────────▼──────────────────────┐
    │ InputConsumer::receive_event()      │
    │ ↓                                   │
    │ parse JSON                          │
    │ ↓                                   │
    │ (InboundEvent, MessageContext)      │
    └──────────────┬──────────────────────┘
                        │
3. PIPELINE (pipeline/processor.rs)
    ┌──────────────▼──────────────────────┐
    │ Processor::normalize + enrich       │
    │ ↓                                   │
    │ EntityPositionUpdate                │
    └──────────────┬──────────────────────┘
                        │
4. OUTPUT ADAPTER (output/producer.rs)
    ┌──────────────▼──────────────────────┐
    │ OutputProducer::publish_event()     │
    │ ↓                                   │
    │ serialize to JSON                   │
    │ ↓                                   │
    │ ProduceRequest { payload: &[u8] }   │
    └──────────────┬──────────────────────┘
                        │
5. INFRAESTRUCTURA (kafka/producer.rs)
    ┌──────────────▼──────────────────────┐
    │ KafkaProducer::send()               │
    │ ↓                                   │
    │ PublishResult { partition, offset } │
    └──────────────┬──────────────────────┘
                        │
6. COMMIT (pipeline/processor.rs)
    ┌──────────────▼──────────────────────┐
    │ if commit_on_produce_success:       │
    │   input.commit_offset(context)      │
    └─────────────────────────────────────┘
```

---

## 📚 Referencias de Código

### Dependencias entre módulos:

```
main.rs
  ├─ config/         (configuración)
  ├─ circuit_breaker/ (resiliencia)
  ├─ kafka/          (infraestructura)
  │   ├─ consumer.rs
  │   └─ producer.rs
  ├─ domain/         (negocio puro)
  │   └─ event.rs
  ├─ input/          (adaptador)
  │   └─ consumer.rs  (usa kafka/consumer)
  ├─ output/         (adaptador)
  │   └─ producer.rs  (usa kafka/producer)
  └─ pipeline/       (orquestación)
      └─ processor.rs (usa input + output)
```

### Reglas de Dependencia:

✅ **Permitido**:
- `input/` → `kafka/` + `domain/`
- `output/` → `kafka/` + `domain/`
- `pipeline/` → `input/` + `output/` + `domain/`

❌ **Prohibido**:
- `domain/` → `kafka/` (dominio no conoce infraestructura)
- `kafka/` → `domain/` (infraestructura es genérica)
- `kafka/` → `input/` o `output/` (infraestructura no conoce adaptadores)

---

## 🚀 Próximos Pasos

1. **Enrichers**: Crear módulos en `enrichers/` que se integren en `pipeline/`
2. **Métricas**: Agregar observabilidad sin acoplar a la lógica
3. **Tests de Integración**: Probar flujo completo con Testcontainers
4. **Dead Letter Queue**: Manejar mensajes fallidos sin tocar el core

---

Este diseño permite que cada capa evolucione independientemente, facilitando el testing, mantenimiento y escalabilidad del sistema.
