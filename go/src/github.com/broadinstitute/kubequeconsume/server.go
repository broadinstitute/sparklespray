package kubequeconsume

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/broadinstitute/kubequeconsume/pb"
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
	stdoutPath, ok := m.logPerTaskId[in.TaskId]
	m.mutex.Unlock()

	if !ok {
		return nil, errors.New("Unknown task")
	}

	f, err := os.Open(stdoutPath)
	if err != nil {
		return nil, err
	}

	defer f.Close()

	log.Printf("reading %d bytes from %s (offset %d)", in.Size, stdoutPath, in.Offset)
	buffer := make([]byte, in.Size)
	n, err := f.ReadAt(buffer, in.Offset)
	buffer = buffer[:n]
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &pb.ReadOutputReply{Data: buffer, EndOfFile: err == io.EOF}, nil
}

// Returns error or blocks
func (m *Monitor) StartServer(lis net.Listener) error {
	s := grpc.NewServer()
	pb.RegisterMonitorServer(s, m)

	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return err
	}
	return nil
}

func (m *Monitor) StartWatchingLog(taskId string, stdoutPath string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.logPerTaskId[taskId] = stdoutPath
}
