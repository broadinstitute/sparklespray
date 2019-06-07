FROM golang:1.12.5-alpine3.9
RUN apk add git
RUN go get -u github.com/googlecloudplatform/gcsfuse

FROM alpine:3.9
RUN apk add --no-cache ca-certificates fuse bash && rm -rf /tmp/*
COPY --from=0 /go/bin/gcsfuse /usr/local/bin
WORKDIR /
ADD sparkles-consume-setup /usr/local/bin
