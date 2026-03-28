# Stage 1: Build
FROM golang:1.25 AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /sparklesworker \
    ./cmd/sparklesworker

# Stage 2: Minimal runtime image
FROM gcr.io/distroless/static-debian12

COPY --from=builder /sparklesworker /sparklesworker

ENTRYPOINT ["/sparklesworker"]
