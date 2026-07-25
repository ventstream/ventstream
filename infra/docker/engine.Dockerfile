# Multi-stage build for the VentStream engine binary.
#
# Stage layout:
#   chef     — pin toolchain + install cargo-chef (dependency planner)
#   planner  — compute the dependency "recipe" from the manifests
#   builder  — cook deps (cached layer) then build the `ventstream` bin
#   runtime  — distroless glibc + CA roots, just the binary
#
# Why cargo-chef: without it, any source change busts the dependency
# layer and rebuilds the whole crates.io graph (~minutes). chef cooks
# dependencies in their own layer keyed only on Cargo.{toml,lock}, so
# code edits rebuild in seconds.
#
# Why a glibc runtime (not scratch/musl): the engine uses rustls+ring
# (no OpenSSL) but resolves trust roots via rustls-native-certs, which
# reads the OS trust store. Distroless cc supplies glibc and CA roots without
# a shell, package manager, or the general-purpose utilities that expand the
# runtime vulnerability surface.

# Keep this exact version aligned with rust-toolchain.toml. A floating minor
# tag can make cargo-chef cook dependencies with one compiler and rebuild all
# of them after COPY activates the repository's pinned compiler.
ARG RUST_VERSION=1.94.0
ARG DEBIAN_SNAPSHOT=20260701T000000Z
ARG CARGO_CHEF_VERSION=0.1.77
ARG CARGO_AUDITABLE_VERSION=0.7.5

# ─── chef: toolchain + planner tool ──────────────────────────────────
FROM rust:${RUST_VERSION}-slim-bookworm@sha256:a86cada82e36ebd7a9bffed7548792c55a952fdb20718eea9278a936bcb76e62 AS chef
ARG RUST_VERSION
ARG DEBIAN_SNAPSHOT
ARG CARGO_CHEF_VERSION
ARG CARGO_AUDITABLE_VERSION
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}
# ring compiles C + assembly; cargo-chef + the build need a C toolchain.
# clang + libclang are required by rdkafka-sys (bindgen) for the Kafka/Redpanda
# source's vendored librdkafka build.
RUN sed -i \
      -e "s|http://deb.debian.org/debian-security|http://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT}|" \
      -e "s|http://deb.debian.org/debian|http://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT}|" \
      /etc/apt/sources.list.d/debian.sources \
 && printf 'Acquire::Check-Valid-Until "false";\n' >/etc/apt/apt.conf.d/99snapshot \
 && apt-get update \
 && apt-get install -y --no-install-recommends build-essential=12.9 pkg-config=1.8.1-1 clang=1:14.0-55.7~deb12u1 libclang-dev=1:14.0-55.7~deb12u1 \
 && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked --version "${CARGO_CHEF_VERSION}"
RUN cargo install cargo-auditable --locked --version "${CARGO_AUDITABLE_VERSION}"
WORKDIR /app

# ─── planner: derive the dependency recipe ───────────────────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ─── builder: cook deps (cached) then build the binary ───────────────
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# This layer is cached unless Cargo.lock / a Cargo.toml changes.
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
RUN cargo auditable build --release --locked -p ventstream \
 && strip target/release/ventstream \
 && mkdir -p /runtime/var/lib/ventstream/state

# ─── runtime: distroless image, binary only ──────────────────────────
FROM gcr.io/distroless/cc-debian12:nonroot@sha256:ce0d66bc0f64aae46e6a03add867b07f42cc7b8799c949c2e898057b7f75a151 AS runtime

COPY --from=builder --chown=10001:10001 /app/target/release/ventstream /usr/local/bin/ventstream
COPY --from=builder --chown=10001:10001 /runtime/var/lib/ventstream /var/lib/ventstream

# State + spec files are mounted at runtime (PVC for cursor/redb state,
# ConfigMap/Secret for the joins/denormalize YAML and TLS cert). The
# defaults below are sensible mount points the k8s manifests use.
ENV VS_JOINS_STATE_DIR=/var/lib/ventstream/state \
    VS_NEO4J_STATE_DIR=/var/lib/ventstream/state \
    VS_DLQ_PATH=/var/lib/ventstream/dlq.jsonl \
    RUST_LOG=info \
    VS_LOG_FORMAT=json \
    _RJEM_MALLOC_CONF=background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:1000
# ^ jemalloc tuning. tikv-jemalloc builds with the `_rjem_` symbol prefix,
# so its config env is `_RJEM_MALLOC_CONF` (NOT the unprefixed MALLOC_CONF,
# which it ignores). A background thread returns freed pages to the OS
# after a short decay, so RSS tracks the live working set instead of the
# high-water mark — key for the WS gateway under connection churn. Override
# per-deploy via the `_RJEM_MALLOC_CONF` env if you want different timing.
USER 10001:10001

# Optional listeners (admin / ws / graphql) — only bound when the
# corresponding VS_*_LISTEN env is set. Declared for documentation;
# the k8s Service decides what's actually exposed.
EXPOSE 8080 8081 8082

ENTRYPOINT ["/usr/local/bin/ventstream"]
