# Build stage with cargo-chef for better layer caching
# Native build: each runner builds for its own architecture
FROM lukemathwalker/cargo-chef:latest-rust-1.88.0-slim-bookworm AS chef
WORKDIR /app

# Install system dependencies
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies
FROM chef AS builder
ARG TARGETPLATFORM
ARG BUILDPLATFORM
RUN echo "Native build on $BUILDPLATFORM for $TARGETPLATFORM"

# Install build dependencies including lld linker for faster linking
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    make \
    lld && \
    rm -rf /var/lib/apt/lists/*

# Copy cargo config for build optimizations (lld linker, etc.)
COPY .cargo/config.toml /app/.cargo/config.toml

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build application with optimizations
# The release profile in .cargo/config.toml handles: thin LTO, strip, opt-level=3
COPY . .

# Set sqlx to offline mode (migrations are in separate SQL files)
ENV SQLX_OFFLINE=true

RUN cargo build --release --bin postgres-stream && \
    strip target/release/postgres-stream

# Runtime stage with distroless for security
FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app

# Create non-root user (distroless already has nonroot user)
USER nonroot:nonroot

# Copy binary and configuration files
COPY --from=builder /app/target/release/postgres-stream ./postgres-stream
COPY configuration/ ./configuration/

# Use exec form for proper signal handling
ENTRYPOINT ["./postgres-stream"]

