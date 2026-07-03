# syntax=docker/dockerfile:1
FROM rust:slim AS builder
RUN apt-get update && \
    apt-get install -y build-essential musl-tools && \
    rm -rf /var/lib/apt/lists/* && \
    rustup target add x86_64-unknown-linux-musl
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN cargo fetch
COPY src/ src/
RUN cargo build --release --target x86_64-unknown-linux-musl --frozen

FROM alpine:latest
RUN adduser -D -u 1000 app && \
    mkdir -p /var/cache/apt-blitz && \
    chown app:app /var/cache/apt-blitz
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/apt-blitz /usr/local/bin/
USER app
EXPOSE 8080
VOLUME /var/cache/apt-blitz
ENTRYPOINT ["apt-blitz"]
