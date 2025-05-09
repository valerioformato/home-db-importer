FROM rust:1.71-slim-bullseye as builder

# Create a new empty shell project
WORKDIR /usr/src/home-db-importer
COPY . .

# Build your program for release
RUN cargo build --release

# Create a new stage with a minimal image
FROM debian:bullseye-slim

# Install OpenSSL - required for HTTPS requests
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /usr/src/home-db-importer/target/release/home-db-importer /usr/local/bin/home-db-importer

# Create directory for state files that can be mounted as a volume
WORKDIR /data

# Command to run the executable
ENTRYPOINT ["home-db-importer"]
