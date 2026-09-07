# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod .
COPY *.go .
# CGO を無効化してスタティックリンクした Linux バイナリを生成する。
RUN CGO_ENABLED=0 GOOS=linux go build -o processor .

FROM alpine:3.20

# CIS Docker Benchmark 4.1 準拠: 非 root ユーザを作成
RUN adduser -D -H -s /sbin/nologin app

WORKDIR /app
COPY --from=builder --chown=app:app /app/processor .

USER app

EXPOSE 8002
CMD ["./processor"]
