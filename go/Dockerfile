# Stage 1: Build the Go binary
FROM golang:1.19 AS builder

WORKDIR /app
COPY src src
WORKDIR /app/src/github.com/broadinstitute/sparklesworker/cli
# build staticly linked executable
RUN GCO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-linkmode 'external' -extldflags '-static'" -o /app/sparklesworker .

#RUN cp /app/sparklesworker /sparklesworker

# Stage 2: Create the minimal image
FROM gcr.io/distroless/static-debian11
#FROM alpine:3.14

COPY --from=builder /app/sparklesworker /sparklesworker
ENTRYPOINT ["/sparklesworker"]
