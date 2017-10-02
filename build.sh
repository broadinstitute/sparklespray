PWD=`pwd`
docker run --rm -v $PWD/go:/go \
    -v $PWD/kubeque/bin:/dest-bin \
    -w /go/src/github.com/broadinstitute/kubequeconsume/cli golang:1.9 \
    bash -c 'CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o /dest-bin/kubequeconsume github.com/broadinstitute/kubequeconsume/cli'


