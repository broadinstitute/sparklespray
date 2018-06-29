package kubequeconsume

import (
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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
func (m *Monitor) StartServer(port string, certPEMBlock []byte, keyPEMBlock []byte, sharedSecret string) error {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("could not listen on %s: %v\n", port, err)
		return err
	}

	tlsCert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return err
	}

	creds := credentials.NewServerTLSFromCert(&tlsCert)

	AuthInterceptor := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		keys, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			log.Printf("no meta")
			return nil, errors.New("missing meta")
		}
		t := keys.Get("shared-secret")[0]
		if t == "" {
			return nil, grpc.Errorf(codes.Unauthenticated, "missing shared-secret")
		}
		secretFromClient := t //.(string)
		if sharedSecret != secretFromClient {
			return nil, grpc.Errorf(codes.Unauthenticated, "incorrect shared-secret")
		}
		return handler(ctx, req)
	}

	s := grpc.NewServer(grpc.Creds(creds), grpc.UnaryInterceptor(AuthInterceptor))
	pb.RegisterMonitorServer(s, m)

	// Register reflection service on gRPC server.
	reflection.Register(s)

	start := func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("failed to serve: %v", err)
		}
	}

	go start()

	return nil
}

func (m *Monitor) StartWatchingLog(taskId string, stdoutPath string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.logPerTaskId[taskId] = stdoutPath
}
