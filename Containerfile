FROM docker.io/library/node:22-bookworm-slim AS web-build
WORKDIR /source/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM docker.io/library/rust:1.88-bookworm AS rust-build
WORKDIR /source
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY --from=web-build /source/web/dist ./web/dist
RUN cargo build --locked --release

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 nva2dlna \
    && useradd --system --uid 10001 --gid 10001 --home-dir /app --shell /usr/sbin/nologin nva2dlna \
    && mkdir -p /app/data \
    && chown 10001:10001 /app/data
WORKDIR /app
COPY --from=rust-build /source/target/release/nva2dlna /usr/local/bin/nva2dlna
COPY --from=web-build /source/web/dist /app/web/dist
VOLUME ["/app/data"]
EXPOSE 8080/tcp 9958/tcp 52288/tcp 1900/udp 25353/udp
USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/nva2dlna"]
