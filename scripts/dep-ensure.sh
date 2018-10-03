PWD=`pwd`

if [ ! -e go/src/github.com/broadinstitute/kubequeconsume/vendor ] ; then
    export GOPATH=$PWD/go
    cd go/src/github.com/broadinstitute/kubequeconsume
    dep ensure "$@"
    mv ./go/src/github.com/broadinstitute/kubequeconsume/vendor/cloud.google.com/go/storage/go110.go.pending ./go/src/github.com/broadinstitute/kubequeconsume/vendor/cloud.google.com/go/storage/go110.go
fi
