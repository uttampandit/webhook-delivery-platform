FROM node:26.13.0-bookworm-slim AS web-build
WORKDIR /src/web
RUN npm install --global npm@11.20.0
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

FROM golang:1.27.3-bookworm AS go-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY migrations/ ./migrations/
RUN mkdir -p /out && \
	for binary in api worker receiver seed migrate democtl benchmark; do \
      CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o "/out/${binary}" "./cmd/${binary}"; \
    done

FROM debian:bookworm-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 webhook && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin webhook
WORKDIR /app
COPY --from=go-build --chown=10001:10001 /out/ /app/
COPY --from=go-build --chown=10001:10001 /src/migrations/ /app/migrations/
COPY --from=web-build --chown=10001:10001 /src/web/dist/ /app/web/
ENV WEB_STATIC_DIR=/app/web
USER 10001:10001
ENTRYPOINT ["/app/api"]
