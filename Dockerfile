# Shopping Sessions: a single Rust binary. The scorer builds this natively on linux/amd64.
# Base images pinned by digest (the tags' digests on 2026-11-22), so a later rebuild
# uses the same toolchain and runtime as the tested one.
FROM rust:1-slim-bookworm@sha256:7c7d4eb4d05b7781187a327809466cfddf29753f6defdc3d19a0e291951b7a37 AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2) runs on every scorer CPU (Cascade Lake or newer).
RUN if [ "$(uname -m)" = "x86_64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release && cp target/release/sessions /sessions

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /sessions /app/sessions
# Large malloc allocations on transparent huge pages (see main.rs: the binary
# restarts itself without this if the machine lacks free 2 MB blocks).
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000
CMD ["/app/sessions"]
