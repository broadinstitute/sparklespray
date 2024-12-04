DEST=`pwd`
cd src/github.com/broadinstitute/sparklesworker/cli
GCO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-linkmode 'external' -extldflags '-static'" -o ${DEST}/sparklesworker .
