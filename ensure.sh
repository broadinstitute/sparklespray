PWD=`pwd`

export GOPATH=$PWD/go
cd go/src/github.com/broadinstitute/kubequeconsume
dep ensure "$@"
