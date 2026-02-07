# DevContainer para GeoContext

Configuración de Development Container para el microservicio GeoContext en Rust.

## 🎯 Características

- **Ubuntu 22.04** como base
- **Rust** instalado con rustup (última versión estable)
- **Componentes**: rustfmt, clippy, rust-analyzer
- **Network**: Conectado a `siscom-network` para acceso a Kafka/Redpanda
- **Variables de entorno**: Carga automática desde `.env`
- **Build automático**: Compila el proyecto al crear el contenedor

## 🚀 Uso

### Prerrequisitos

1. Docker instalado y corriendo
2. Red Docker `siscom-network` creada:
   ```bash
   docker network create siscom-network
   ```
3. VS Code con extensión "Dev Containers" instalada

### Iniciar el DevContainer

1. Abrir VS Code en la carpeta del proyecto
2. Presionar `F1` o `Cmd+Shift+P`
3. Seleccionar: **"Dev Containers: Reopen in Container"**
4. Esperar a que se construya la imagen y compile el proyecto

### Primera vez

El script `start-geocontext.sh` se ejecuta automáticamente y:
- ✅ Carga variables de entorno desde `.env`
- ✅ Verifica la instalación de Rust
- ✅ Compila el proyecto en modo release
- ✅ Muestra comandos disponibles

## 📝 Comandos Disponibles

Dentro del contenedor:

```bash
# Ejecutar en modo desarrollo
cargo run

# Ejecutar en modo optimizado
cargo run --release

# Ejecutar tests
cargo test

# Ejecutar tests con output
cargo test -- --nocapture

# Linter
cargo clippy

# Formatear código
cargo fmt

# Verificar formato sin modificar
cargo fmt -- --check

# Build release
cargo build --release
```

## 🔧 Configuración

### Variables de Entorno

Las variables se cargan automáticamente desde `.env`:
- Kafka brokers, topics, credenciales
- Circuit breaker config
- H3 resolution
- Logging level

Para modificar configuración:
1. Editar `.env` en el workspace
2. Recargar variables: `source .env` (si es necesario)
3. Reiniciar la aplicación

### Red Docker

El contenedor se conecta a la red externa `siscom-network`:
```yaml
networks:
  siscom-network:
    external: true
```

Esto permite comunicación con otros servicios en la misma red (ej: Redpanda/Kafka).

## 🏗️ Estructura

```
.devcontainer/
├── devcontainer.json       # Configuración del devcontainer
├── docker-compose.yml      # Servicio Docker Compose
├── Dockerfile              # Imagen Ubuntu + Rust
├── start-geocontext.sh     # Script de inicialización
└── README.md               # Este archivo
```

## 🔌 Extensiones de VS Code

Se instalan automáticamente:

**Rust:**
- rust-analyzer (análisis de código)
- vscode-lldb (debugging)
- crates (gestión de dependencias)

**DevOps:**
- Docker
- YAML support
- TOML support

**Productividad:**
- GitLens
- Better Comments
- Code Spell Checker

## 🐛 Debugging

La extensión vscode-lldb permite debugging:

1. Colocar breakpoints en el código
2. Presionar `F5` o ir a "Run and Debug"
3. Seleccionar configuración de debug
4. El debugger se adjuntará al proceso

## 🛠️ Troubleshooting

### El contenedor no se conecta a Kafka

Verificar que:
1. La red `siscom-network` existe: `docker network ls`
2. Kafka/Redpanda está en la misma red
3. El hostname en `.env` es correcto (usar nombre del servicio Docker)

```bash
# Verificar conectividad desde el contenedor
docker exec geocontext-dev ping redpanda
```

### Variables de entorno no se cargan

1. Verificar que `.env` existe en la raíz del proyecto
2. Reconstruir el contenedor: "Dev Containers: Rebuild Container"

### Error al compilar

1. Verificar dependencias del sistema:
   ```bash
   # Dentro del contenedor
   sudo apt-get update
   sudo apt-get install -y build-essential pkg-config libssl-dev libsasl2-dev
   ```

2. Limpiar y recompilar:
   ```bash
   cargo clean
   cargo build --release
   ```

### Rust no se encuentra

Verificar PATH:
```bash
source $HOME/.cargo/env
echo $PATH
```

## 📚 Referencias

- [VS Code Dev Containers](https://code.visualstudio.com/docs/devcontainers/containers)
- [Rust Book](https://doc.rust-lang.org/book/)
- [rdkafka](https://docs.rs/rdkafka/)

## 🔄 Actualizar el Contenedor

Después de modificar archivos del devcontainer:

1. Presionar `F1` o `Cmd+Shift+P`
2. Seleccionar: **"Dev Containers: Rebuild Container"**

Esto reconstruirá la imagen con los cambios.
