package kubequeconsume

import (
	"log"
	"net"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Monitor struct {
}

func NewMonitor() *Monitor {
	return &Monitor{}
}

func (s *Monitor) ReadOutput(ctx context.Context, in *pb.ReadOutputRequest) (*pb.ReadOutputReply, error) {
	return &pb.ReadOutputReply{Data: "Hello\n", EndOfFile: true}, nil
}

// Returns error or blocks
func (monitor *Monitor) StartServer(lis net.Listener) error {
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, monitor)

	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return err
	}
	return nil
}
