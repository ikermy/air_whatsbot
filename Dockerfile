# ── Стадия сборки ──────────────────────────────────────────────────────────────
FROM golang:1.26 AS builder

WORKDIR /app

# Копируем модули
COPY go.mod go.sum ./

# Скачиваем зависимости
RUN go mod download

# Копируем исходники
COPY . .

# Собираем статический бинарь
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o whatsbot ./cmd/

RUN apt-get update && apt-get install -y --no-install-recommends upx-ucl \
    && rm -rf /var/lib/apt/lists/*
RUN upx --best --lzma /app/whatsbot

# ── Финальный образ ────────────────────────────────────────────────────────────
FROM alpine:3.22 AS runtime-assets

RUN apk add --no-cache ca-certificates tzdata

FROM scratch

WORKDIR /app

COPY --from=builder /app/whatsbot /app/whatsbot
COPY --from=runtime-assets /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=runtime-assets /usr/share/zoneinfo /usr/share/zoneinfo

ENV TZ=Europe/Amsterdam

ENTRYPOINT ["/app/whatsbot"]
