CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o kubequeconsume github.com/broadinstitute/kubequeconsume/cli
