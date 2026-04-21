# Stage 1: Build the Go binary
FROM golang:1.25 AS builder

WORKDIR /app
COPY src src
WORKDIR /app/src/github.com/broadinstitute/sparklesworker/cli
# build staticly linked executable
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/sparklesworker .

#RUN cp /app/sparklesworker /sparklesworker

# Stage 2: Create the minimal image
FROM gcr.io/distroless/static-debian11

COPY --from=builder /app/sparklesworker /sparklesworker
ENTRYPOINT ["/sparklesworker"]
