FROM rust:1.99.1-slim AS builder
WORKDIR /app

# Cache dependencies before copying source files.
COPY services/ingest/Cargo.toml services/ingest/Cargo.lock services/ingest/
RUN mkdir -p services/ingest/src && echo "fn main() {}" > services/ingest/src/main.rs \
    && cargo build --manifest-path services/ingest/Cargo.toml --release \
    && rm -rf services/ingest/src

# sqlx::migrate!("../../migrations") resolves relative to CARGO_MANIFEST_DIR.
COPY migrations migrations
COPY services/ingest/src services/ingest/src
RUN touch services/ingest/src/main.rs \
    && cargo build --manifest-path services/ingest/Cargo.toml --release

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin taggle

COPY --from=builder /app/services/ingest/target/release/taggle-ingest /usr/local/bin/taggle-ingest
USER taggle
EXPOSE 4001
CMD ["taggle-ingest"]
