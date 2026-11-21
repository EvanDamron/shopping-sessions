# Diagnostic build (not an entry): builds the current pipeline, an older one and the
# public data generator, and times them on generated data inside the dev-set run.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ab ./ab
RUN export RUSTFLAGS="-C target-cpu=x86-64-v3"; \
    cargo build --release && cp target/release/sessions /sessions && \
    cd ab/old && cargo build --release && cp target/release/sessions /sessions_old && \
    cd ../generator && cargo build --release && cp target/release/generate /generate

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /sessions /sessions_old /generate /app/
COPY ab/run.sh /app/run.sh
USER 1000
CMD ["/app/run.sh"]
