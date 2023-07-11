cd go/src/github.com/broadinstitute/kubequeconsume 
# build staticly linked amd64 executable
GOARCH=amd64 CGO_ENABLED=0 GOOS=linux go build \
  -ldflags "-s" -a \
  -installsuffix cgo -o ../../../../../cli/sparklespray/bin/kubequeconsume github.com/broadinstitute/kubequeconsume/cli


