# ---------------------------------------------------
# Stage 1: Build the Rust Binary
# ---------------------------------------------------
FROM rust:1.75-slim-bookworm as builder

WORKDIR /app
# Copy the entire workspace
COPY . .

# Build the native server in release mode
RUN cargo build --release --manifest-path server-native/Cargo.toml

# ---------------------------------------------------
# Stage 2: Minimal Runtime Image
# ---------------------------------------------------
FROM debian:bookworm-slim

# Install CA certificates in case Recached ever needs to make outbound HTTPS requests
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/server-native /usr/local/bin/recached-server

# Expose the standard Redis port (6379) and the WebSocket port (6380)
EXPOSE 6379
EXPOSE 6380

# Run the server
CMD ["recached-server"]
