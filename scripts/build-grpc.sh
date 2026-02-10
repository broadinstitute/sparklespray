set -ex
#export PATH=$PATH:$GOPATH/bin
#protoc -I protobuf/sparklespray/sparklespray protobuf/sparklespray/sparklespray/pb.proto --go_out=go/src/github.com/broadinstitute/sparklesworker --python_out=./cli --mypy_out=./cli/sparklespray --go-grpc_opt=paths=source_relative


protoc -I protobuf/sparklespray/sparklespray \
      --go_out=go/src/github.com/broadinstitute/sparklesworker/pb \
      --go_opt=paths=source_relative \
      --go-grpc_out=go/src/github.com/broadinstitute/sparklesworker/pb \
      --go-grpc_opt=paths=source_relative \
      protobuf/sparklespray/sparklespray/pb.proto \
      --python_out=./cli \
      --mypy_out=./cli/sparklespray
      


#protoc -I protobuf/sparklespray/sparklespray protobuf/sparklespray/sparklespray/pb.proto --go_out=plugins=grpc:go/src/github.com/broadinstitute/kubequeconsume/pb
#protoc -I sparklespray/ sparklespray/pb.proto --python_out kubeque/grpc_client
#python -m grpc_tools.protoc -I protobuf/sparklespray --python_out=./cli --grpc_python_out=./cli 'protobuf/sparklespray/sparklespray/pb.proto'
#protoc -I protobuf/sparklespray/sparklespray  protobuf/sparklespray/sparklespray/pb.proto --python_out=./cli --mypy_out=./cli
 
