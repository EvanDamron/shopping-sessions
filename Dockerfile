# Shopping Sessions: a single Rust binary. The scorer builds this natively on linux/amd64.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 (AVX2) runs on every scorer CPU (Cascade Lake or newer).
RUN if [ "$(uname -m)" = "x86_64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi; \
    cargo build --release && cp target/release/sessions /sessions

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /sessions /app/sessions
# Large malloc allocations (row columns, pass buffers) on transparent huge pages: half
# the page faults at 60M. The record arenas and the ID table ask for them in the code.
ENV GLIBC_TUNABLES=glibc.malloc.hugetlb=1
USER 1000
CMD ["/app/sessions"]
