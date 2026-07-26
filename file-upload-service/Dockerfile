# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.91.1
ARG APP_NAME=file-upload-service

###############################
# Chef Base
###############################
FROM rust:${RUST_VERSION}-bookworm AS chef

WORKDIR /app

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    clang \
    lld \
    git \
 && cargo install cargo-chef \
 && rm -rf /var/lib/apt/lists/*

###############################
# Planner
###############################
FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# If you have migrations or build.rs, uncomment these:
# COPY migrations ./migrations
# COPY build.rs .

RUN cargo chef prepare --recipe-path recipe.json

###############################
# Builder
###############################
FROM chef AS builder

ARG APP_NAME

COPY --from=planner /app/recipe.json recipe.json

# Build and cache dependencies
RUN cargo chef cook --release --locked --recipe-path recipe.json

# Copy the full source
COPY . .

# Build the application
RUN cargo build --release --locked

###############################
# Runtime
###############################
FROM debian:bookworm-slim

ARG APP_NAME

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
 && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 10001 appuser

WORKDIR /app

COPY --from=builder /app/target/release/${APP_NAME} /usr/local/bin/server

USER appuser

EXPOSE 8000

CMD ["server"]
