#!/bin/bash

curwd=`pwd`
export GOPATH=$PATH:${curwd}/go
go build go/src/github.com/broadinstitute/kubequeconsume/cli/main.go

