# Base Ubuntu image
FROM ubuntu:22.04

# Evitar prompts interactivos
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \
    libsasl2-dev \
    libzstd-dev \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario vscode
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME \
    && rm -rf /var/lib/apt/lists/*

# Cambiar a usuario vscode
USER $USERNAME

# Instalar Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && echo 'source $HOME/.cargo/env' >> $HOME/.bashrc

# Configurar PATH para Rust
ENV PATH="/home/${USERNAME}/.cargo/bin:${PATH}"

# Instalar componentes de Rust útiles para desarrollo
RUN /home/${USERNAME}/.cargo/bin/rustup component add rustfmt clippy rust-analyzer

# Crear directorio de workspace (necesita permisos de root)
USER root
RUN mkdir -p /workspaces && chown ${USERNAME}:${USERNAME} /workspaces
USER ${USERNAME}

# Script de inicio
COPY --chown=$USERNAME:$USERNAME start-geocontext.sh /usr/local/bin/start-geocontext.sh
RUN chmod +x /usr/local/bin/start-geocontext.sh

WORKDIR /workspaces/GeoContext

# Mantener el contenedor corriendo
CMD ["tail", "-f", "/dev/null"]
