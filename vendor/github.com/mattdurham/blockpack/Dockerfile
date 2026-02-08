FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build temposerver
RUN go build -o temposerver ./cmd/temposerver

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/temposerver .

# Expose default port
EXPOSE 3200

ENTRYPOINT ["./temposerver"]
