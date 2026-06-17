# File: ../agrisentry-iot-gateway/Dockerfile

# Stage 1: Compilation Environment
# Fix: Atualizado de 1.80-slim para slim (latest) para suportar a sintaxe Rust Edition 2024 exigida pelo Cargo.lock.
FROM rust:slim AS builder
WORKDIR /usr/src/agrisentry-iot-gateway

# Purpose: Install musl-tools to enable statically linked binaries.
RUN apt-get update && apt-get install -y musl-tools && rm -rf /var/lib/apt/lists/*
RUN rustup target add x86_64-unknown-linux-musl

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Purpose: Compile the binary explicitly for the musl target. 
RUN cargo build --target x86_64-unknown-linux-musl --release

# Stage 2: Distroless Runtime Environment
FROM scratch

# Purpose: Isolate the runtime environment entirely. 
COPY --from=builder /usr/src/agrisentry-iot-gateway/target/x86_64-unknown-linux-musl/release/agrisentry-iot-gateway /usr/local/bin/agrisentry-iot-gateway

# Security: Enforce execution under an unprivileged user ID. 
USER 1000:1000

# Lifecycle: Execute the statically compiled binary directly without intermediary shells.
ENTRYPOINT ["/usr/local/bin/agrisentry-iot-gateway"]