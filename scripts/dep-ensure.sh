PWD=`pwd`

if [ ! -e go/src/github.com/broadinstitute/kubequeconsume/vendor ] ; then
    export GOPATH=$PWD/go
    cd go/src/github.com/broadinstitute/kubequeconsume
    #dep ensure "$@"
    
    docker run --rm \
        --platform linux/amd64\
        -v $(pwd):/go/src/github.com/broadinstitute/kubequeconsume \
        -w /go/src/github.com/broadinstitute/kubequeconsume \
        -v $(pwd)/.caches/dep:/go/pkg/dep \
        instrumentisto/dep:alpine ensure
    
    mv ./vendor/cloud.google.com/go/storage/go110.go.pending ./vendor/cloud.google.com/go/storage/go110.go
fi
