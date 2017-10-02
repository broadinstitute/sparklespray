docker run --rm -v /Users/pmontgom/go/src:/go/src -w /go/src/github.com/broadinstitute/kubequeconsume/cli golang:1.9 sh build.sh
ls -l /Users/pmontgom/go/src/github.com/broadinstitute/kubequeconsume/cli/kubequeconsume
cp /Users/pmontgom/go/src/github.com/broadinstitute/kubequeconsume/cli/kubequeconsume kubeque/bin/kubequeconsume

