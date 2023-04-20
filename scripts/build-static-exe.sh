PWD=`pwd`
docker run --rm -v $PWD/go:/go2 \
    --platform=linux/amd64 \
    -e GOPATH=/go:/go2 \
    -v $PWD/sparklespray/bin:/dest-bin \
    -w /dest-bin golang:1.11 \
    bash -c 'CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o /dest-bin/kubequeconsume github.com/broadinstitute/kubequeconsume/cli'


