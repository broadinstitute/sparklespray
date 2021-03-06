set -ex
export PATH=$PATH:$GOPATH/bin
protoc -I protobuf/sparklespray/sparklespray protobuf/sparklespray/sparklespray/pb.proto --go_out=plugins=grpc:go/src/github.com/broadinstitute/kubequeconsume/pb
#protoc -I sparklespray/ sparklespray/pb.proto --python_out kubeque/grpc_client
python -m grpc_tools.protoc -I protobuf/sparklespray --python_out=. --grpc_python_out=. 'protobuf/sparklespray/sparklespray/pb.proto'

