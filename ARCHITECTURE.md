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
         └───►│ SiscomMinimalEvent        │◄────┘
              │ SiscomEnrichedEvent       │
              │ GeoContext                │
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
pub struct SiscomMinimalEvent {
    pub data: Value,  // Evento de entrada
}

pub struct SiscomEnrichedEvent {
    pub original: Value,
    pub geo_context: Option<GeoContext>,  // Enriquecimiento
}

pub struct GeoContext {
    pub h3_index: Option<String>,
    pub region: Option<String>,
    pub metadata: Option<Value>,
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
        -> Result<Option<(SiscomMinimalEvent, MessageContext)>> 
    {
        // 1. Recibe KafkaMessage de la infra
        // 2. Convierte a SiscomMinimalEvent (dominio)
        // 3. Retorna evento + contexto
    }
}
```

**Flujo**:
```
KafkaMessage (bytes) → JSON parsing → SiscomMinimalEvent (dominio)
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
        event: &SiscomEnrichedEvent,  // Dominio
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
SiscomEnrichedEvent (dominio) → JSON serialization → ProduceRequest (bytes) → Kafka
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

1. **Recibir**: `input.receive_event()` → `(SiscomMinimalEvent, MessageContext)`
2. **Enriquecer**: `enrich_event()` → `SiscomEnrichedEvent`
3. **Publicar**: `output.publish_event()` → `PublishResult`
4. **Commit condicional**:
   - Si `commit_on_produce_success=true`: commit solo si publish OK
   - Si `commit_on_produce_success=false`: commit inmediato

```rust
async fn process_single_message(&self) -> Result<bool> {
    // 1. Input adapter: Kafka → Dominio
    let (event, context) = self.input.receive_event().await?;
    
    // 2. Enriquecimiento
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
   │ (SiscomMinimalEvent, MessageContext)│
   └──────────────┬──────────────────────┘
                  │
3. PIPELINE (pipeline/processor.rs)
   ┌──────────────▼──────────────────────┐
   │ Processor::enrich_event()           │
   │ ↓                                   │
   │ SiscomEnrichedEvent                 │
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
