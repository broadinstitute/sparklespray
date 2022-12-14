#!/bin/bash

set -ex

ORIG_PWD=`pwd`

cd go/src/github.com/broadinstitute/kubequeconsume
CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o $ORIG_PWD/dest-bin/kubequeconsume cli/main.go

#docker run --rm -v $PWD/go:/go2 \
#    -e GOPATH=/go:/go2 \
#    -v $PWD/sparklespray/bin:/dest-bin \
#    -w /dest-bin golang:1.11 \
#    bash -c 'CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o /dest-bin/kubequeconsume github.com/broadinstitute/kubequeconsume/cli'


