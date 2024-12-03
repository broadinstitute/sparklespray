cd go/src/github.com/broadinstitute/sparklesworker 
# build staticly linked amd64 executable
GOARCH=amd64 CGO_ENABLED=0 GOOS=linux go build \
  -ldflags "-s" -a \
  -installsuffix cgo -o ../../../../../cli/sparklespray/bin/sparklesworker github.com/broadinstitute/sparklesworker/cli


