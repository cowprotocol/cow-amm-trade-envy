# Build arguments for version and base image selection.
ARG UV_VERSION=0.5.13
ARG DEBIAN_VERSION=bookworm

# Stage 1: Pull the UV base image.
FROM ghcr.io/astral-sh/uv:$UV_VERSION AS uv

# Stage 2: Build the final development container image.
FROM mcr.microsoft.com/vscode/devcontainers/base:$DEBIAN_VERSION
LABEL maintainer="a5chin <a5chin.origin+contact@gmain.com>"

# Install necessary system dependencies.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    libgl1 libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy files from the UV image to the final image.
COPY --from=uv --chown=vscode: /uv /uvx /bin/

# Set default working directory for the container.
WORKDIR /workspaces

# Set default user to vscode.
USER vscode
