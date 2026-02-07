#!/bin/bash
set -e

echo "🚀 Starting GeoContext setup..."

# Verificar que estamos en el directorio correcto
if [ ! -f "Cargo.toml" ]; then
    echo "❌ Error: Cargo.toml not found in workspace"
    exit 1
fi

# Cargar variables de entorno del .env
if [ -f ".env" ]; then
    echo "📝 Loading environment variables from .env..."
    export $(grep -v '^#' .env | xargs)
else
    echo "⚠️  Warning: .env file not found"
fi

# Verificar versión de Rust
echo "🦀 Rust version:"
rustc --version
cargo --version

# Compilar el proyecto
echo "🔨 Building GeoContext in release mode..."
cargo build --release

if [ $? -eq 0 ]; then
    echo "✅ Build successful!"
    echo ""
    echo "📚 Available commands:"
    echo "  cargo run               - Run in development mode"
    echo "  cargo run --release     - Run optimized build"
    echo "  cargo test              - Run all tests"
    echo "  cargo clippy            - Run linter"
    echo "  cargo fmt               - Format code"
    echo ""
    echo "🚀 To start the service:"
    echo "  cargo run --release"
else
    echo "❌ Build failed"
    exit 1
fi
