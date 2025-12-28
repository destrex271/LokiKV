# Stage 1: Build the application
FROM rust:1-slim as builder

# Install cargo-chef for dependency caching
RUN apt-get update && apt-get install -y protobuf-compiler libssl-dev pkg-config
RUN cargo install cargo-chef 

# Set the working directory
WORKDIR /app

# Copy the dependency manifests
COPY Cargo.toml Cargo.lock ./
COPY control.toml .
COPY . .

# Build the release binary
RUN cargo build --release --bin server-db

# Stage 2: Create the runtime image
FROM gcr.io/distroless/cc-debian12

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/server-db /usr/local/bin/server-db

# Copy the control file from the builder stage
COPY --from=builder /app/control.toml /app/control.toml

# Set the environment variable for the control file path
ENV CONTROL_FILE_PATH="/app/control.toml"

# Create and set up a volume for data persistence
VOLUME /app/data

# Set the command to run the server
CMD ["/usr/local/bin/server-db"]
