# One image with every binary: the API (`flash-sale`) and the admission,
# payment and cleanup workers. docker-compose.yml picks the command.

FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
# Caches keep rebuilds incremental; binaries are copied out of the cache mount
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bins && \
    mkdir -p /out && \
    cp target/release/flash-sale target/release/admission_worker \
       target/release/payment_worker target/release/cleanup_worker /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends libpq5 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
ENV RUST_LOG=info
CMD ["flash-sale"]
