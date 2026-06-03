# 🚀 Quick Start Guide - GeoContext

Guía rápida para poner en marcha el microservicio GeoContext en menos de 5 minutos.

## Prerrequisitos

- ✅ Docker Desktop instalado y corriendo
- ✅ VS Code instalado
- ✅ Extensión "Dev Containers" de VS Code

## Paso 1: Clonar y Abrir el Proyecto

```bash
git clone <repo-url>
cd GeoContext
code .
```

## Paso 2: Crear Red Docker

```bash
# Crear la red siscom-network
docker network create siscom-network
```

O usando Makefile:
```bash
make network
```

## Paso 3: Abrir en DevContainer

En VS Code:

1. Presiona `F1` (o `Cmd+Shift+P` en Mac)
2. Escribe: "Dev Containers: Reopen in Container"
3. Presiona Enter

**Esto tomará unos minutos la primera vez** mientras:
- 🐳 Construye la imagen Docker (Ubuntu + Rust)
- 📦 Instala Rust y herramientas
- 🔨 Compila el proyecto
- ✅ Ejecuta el script de inicialización

## Paso 4: Verificar la Instalación

Una vez dentro del contenedor, verás el output del script de inicio:

```
🚀 Starting GeoContext setup...
📝 Loading environment variables from .env...
🦀 Rust version:
rustc 1.xx.x
cargo 1.xx.x
🔨 Building GeoContext in release mode...
✅ Build successful!
```

## Paso 5: Ejecutar el Microservicio

### Opción A: Comando directo
```bash
cargo run --release
```

### Opción B: Usando Makefile
```bash
make run
```

Deberías ver:
```
INFO Starting geocontext microservice with hexagonal architecture
INFO Configuration loaded successfully
INFO Initializing infrastructure layer (Kafka)
INFO Initializing adapter layer (Input/Output)
INFO Initializing application layer (Pipeline)
INFO All layers initialized, starting processing loop
```

## Paso 6: Producir Mensajes de Prueba

En otra terminal (fuera del contenedor), conecta a Kafka:

```bash
# Producir mensajes al topic de entrada
docker exec -it <kafka-container> kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic siscom-minimal

# Pegar este mensaje:
{"uuid":"veh-123","latitude":"19.4326","longitude":"-99.1332","gps_datetime":"2024-01-15 10:30:00","speed":"36.0","received_at":1770444644983}
```

## Paso 7: Verificar Salida

En otra terminal, consumir del topic de salida:

```bash
docker exec -it <kafka-container> kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic entity-position-updates \
  --from-beginning
```

Deberías ver:
```json
{
  "source": "gps",
  "device_id": "veh-123",
  "lat": 19.4326,
  "lon": -99.1332,
  "recorded_at": "2024-01-15 10:30:00",
  "received_at": "1770444644983",
  "speed_mps": 10.0,
  "h3_10": "8a2a1072b59ffff",
  "h3_10_ring_1": ["8a2a1072b58ffff", "8a2a1072b4fffff"],
  "h3_9": "892a1072b5bffff",
  "h3_8": "882a1072b5fffff",
  "h3_7": "872a1072bffffff"
}
```

---

## 🛠️ Comandos Útiles (dentro del contenedor)

```bash
# Ejecutar tests
make test

# Ver logs en modo debug
RUST_LOG=trace cargo run

# Compilar sin ejecutar
make build

# Formatear código
make fmt

# Ejecutar linter
make lint

# Ver todos los comandos disponibles
make help
```

## 📝 Configuración

Edita el archivo `.env` en la raíz del proyecto para cambiar:

- 🔌 Conexión a Kafka (brokers, topics, credenciales)
- 🛡️ Circuit breaker (thresholds, timeouts)
- 🌍 H3 geoespacial (r10 base + r9→r7 derivados, y vecinos `h3_10_ring_1`)
- 📊 Nivel de logging

Nota: en entornos reales usa los topics definidos por `KAFKA_INPUT_TOPIC_SISCOM`, `KAFKA_INPUT_TOPIC_MOBILITY` y `KAFKA_OUTPUT_TOPIC_ENTITY_POSITION` en tu `.env`.

Después de editar, reinicia el servicio (`Ctrl+C` y `cargo run --release`).

## 🐛 Troubleshooting

### No se puede conectar a Kafka

Verifica que Kafka/Redpanda esté en la misma red:
```bash
docker network inspect siscom-network
```

Debe mostrar tanto el contenedor de geocontext como el de Kafka.

### Variables de entorno no se cargan

Verifica que el archivo `.env` existe:
```bash
ls -la .env
```

Si no existe, cópialo desde `.env.example`:
```bash
cp .env.example .env
```

Luego reconstruye el contenedor:
- `F1` → "Dev Containers: Rebuild Container"

### Error de compilación

Limpia y recompila:
```bash
make clean
make build
```

---

## 🎯 Siguiente Paso

Ahora que tienes el microservicio corriendo:

1. 📖 Lee [README.md](README.md) para entender la arquitectura
2. 🧪 Ejecuta `make test` para ver los tests
3. 📝 Revisa [examples/messages.md](examples/messages.md) para más ejemplos
4. 🔧 Personaliza `.env` según tu entorno

## 💡 Tips

- Usa `Ctrl+` (backtick) para abrir la terminal integrada en VS Code
- El contenedor persiste cambios en el código (usa volumes)
- Puedes usar el debugger de VS Code (`F5`)
- `make watch` recompila automáticamente al guardar archivos

---

**¡Listo!** 🎉 Tienes GeoContext corriendo en un DevContainer.
