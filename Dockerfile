# Diagnostic build (not an entry): the current pipeline built for x86-64-v3 and for
# cascadelake, and the public data generator, timed on generated data inside the
# dev-set run.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ab ./ab
RUN RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release && cp target/release/sessions /sessions && \
    RUSTFLAGS="-C target-cpu=cascadelake" cargo build --release --target-dir /tmp/clx && cp /tmp/clx/release/sessions /sessions_clx && \
    cd ab/generator && cargo build --release && cp target/release/generate /generate

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /sessions /sessions_clx /generate /app/
COPY ab/run.sh /app/run.sh
USER 1000
CMD ["/app/run.sh"]
