package kubequeconsume

import (
	"errors"
	"log"
	"net"
	"os"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Monitor struct {
	mutex        sync.Mutex
	logPerTaskId map[string]string
}

func NewMonitor() *Monitor {
	return &Monitor{logPerTaskId: make(map[string]string)}
}

func (m *Monitor) ReadOutput(ctx context.Context, in *pb.ReadOutputRequest) (*pb.ReadOutputReply, error) {
	m.mutex.Lock()
	stdoutPath, ok := m.mutex[in.TaskId]
	m.mutex.Unlock()

	if !ok {
		return nil, errors.New("Unknown task")
	}

	f, err := os.Open(stdoutPath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	buffer := make([]byte, in.Size)
	n, err := f.ReadAt(buffer, in.Offset)
	if err != nil {
		return &pb.ReadOutputReply{EndOfFile: true}, nil
	}
	buffer = buffer[:n]

	return &pb.ReadOutputReply{Data: buffer, EndOfFile: false}, nil
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

func (monitor *Monitor) StartWatchingLog(taskId, stdoutPath) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.logPerTaskId[taskId] = stdoutPath
}
