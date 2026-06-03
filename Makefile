.PHONY: help build run test clean fmt lint check docker-up docker-down network hooks-install hooks-uninstall

# Colores para output
GREEN  := \033[0;32m
YELLOW := \033[0;33m
NC     := \033[0m # No Color

help: ## Mostrar esta ayuda
	@echo "$(GREEN)GeoContext - Comandos disponibles:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(YELLOW)%-20s$(NC) %s\n", $$1, $$2}'

build: ## Compilar en modo release
	@echo "$(GREEN)Compilando GeoContext...$(NC)"
	cargo build --release

build-dev: ## Compilar en modo desarrollo
	@echo "$(GREEN)Compilando en modo dev...$(NC)"
	cargo build

run: ## Ejecutar la aplicación
	@echo "$(GREEN)Ejecutando GeoContext...$(NC)"
	cargo run --release

run-dev: ## Ejecutar en modo desarrollo
	@echo "$(GREEN)Ejecutando en modo dev...$(NC)"
	cargo run

test: ## Ejecutar todos los tests
	@echo "$(GREEN)Ejecutando tests...$(NC)"
	cargo test

test-verbose: ## Ejecutar tests con output detallado
	@echo "$(GREEN)Ejecutando tests (verbose)...$(NC)"
	cargo test -- --nocapture

test-h3: ## Ejecutar tests del enricher H3
	@echo "$(GREEN)Ejecutando tests de H3...$(NC)"
	cargo test enrichers::h3

clean: ## Limpiar artefactos de compilación
	@echo "$(GREEN)Limpiando...$(NC)"
	cargo clean

fmt: ## Formatear código
	@echo "$(GREEN)Formateando código...$(NC)"
	cargo fmt

fmt-check: ## Verificar formato sin modificar
	@echo "$(GREEN)Verificando formato...$(NC)"
	cargo fmt -- --check

lint: ## Ejecutar clippy (linter)
	@echo "$(GREEN)Ejecutando clippy...$(NC)"
	cargo clippy --all-targets --all-features -- -D warnings

check: fmt-check lint test ## Verificar todo (formato, lint, tests)
	@echo "$(GREEN)✅ Todas las verificaciones pasaron!$(NC)"

watch: ## Compilar automáticamente al detectar cambios
	@echo "$(GREEN)Watching for changes...$(NC)"
	cargo watch -x build

watch-test: ## Ejecutar tests automáticamente al detectar cambios
	@echo "$(GREEN)Watching tests...$(NC)"
	cargo watch -x test

# Comandos Docker

network: ## Crear red Docker siscom-network
	@echo "$(GREEN)Creando red siscom-network...$(NC)"
	-docker network create siscom-network

docker-build: ## Construir imagen del devcontainer
	@echo "$(GREEN)Construyendo imagen Docker...$(NC)"
	cd .devcontainer && docker-compose build

docker-up: network ## Levantar el devcontainer
	@echo "$(GREEN)Levantando devcontainer...$(NC)"
	cd .devcontainer && docker-compose up -d

docker-down: ## Detener el devcontainer
	@echo "$(GREEN)Deteniendo devcontainer...$(NC)"
	cd .devcontainer && docker-compose down

docker-logs: ## Ver logs del contenedor
	@echo "$(GREEN)Mostrando logs...$(NC)"
	docker logs -f geocontext-dev

docker-exec: ## Abrir shell en el contenedor
	@echo "$(GREEN)Abriendo shell en el contenedor...$(NC)"
	docker exec -it geocontext-dev /bin/bash

docker-rebuild: ## Reconstruir el contenedor desde cero
	@echo "$(GREEN)Reconstruyendo contenedor...$(NC)"
	cd .devcontainer && docker-compose down
	cd .devcontainer && docker-compose build --no-cache
	cd .devcontainer && docker-compose up -d

# Comandos de desarrollo

dev: docker-up docker-logs ## Levantar contenedor y ver logs

size: ## Mostrar tamaño del binario compilado
	@echo "$(GREEN)Tamaño del binario:$(NC)"
	@ls -lh target/release/geocontext 2>/dev/null || echo "No compilado. Ejecuta 'make build' primero"

deps: ## Mostrar árbol de dependencias
	@echo "$(GREEN)Árbol de dependencias:$(NC)"
	cargo tree

update: ## Actualizar dependencias
	@echo "$(GREEN)Actualizando dependencias...$(NC)"
	cargo update

bench: ## Ejecutar benchmarks (si existen)
	@echo "$(GREEN)Ejecutando benchmarks...$(NC)"
	cargo bench

doc: ## Generar documentación
	@echo "$(GREEN)Generando documentación...$(NC)"
	cargo doc --no-deps --open

# Comandos de producción

release: check build ## Preparar release (verificar + compilar)
	@echo "$(GREEN)✅ Release listo en target/release/geocontext$(NC)"
	@make size

install: ## Instalar el binario en el sistema
	@echo "$(GREEN)Instalando geocontext...$(NC)"
	cargo install --path .

# Útiles para CI/CD

ci: fmt-check lint test ## Simular pipeline de CI
	@echo "$(GREEN)✅ CI checks passed!$(NC)"

hooks-install: ## Configurar hooks locales del repositorio (.githooks)
	@echo "$(GREEN)Configurando git hooks path en .githooks...$(NC)"
	git config core.hooksPath .githooks
	@echo "$(GREEN)✅ Hook pre-push habilitado$(NC)"

hooks-uninstall: ## Restaurar hooks por defecto de Git
	@echo "$(GREEN)Restaurando hooks por defecto...$(NC)"
	git config --unset core.hooksPath || true
	@echo "$(GREEN)✅ Hook path restaurado$(NC)"
