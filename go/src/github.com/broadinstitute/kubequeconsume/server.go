package kubequeconsume

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

type MemoryUsage struct {
	totalSize, totalData, totalShared, totalResident int64 // sum of size of each process visible
	procCount                                        int   // number of processes visible
}

func getMemoryUsage() (*MemoryUsage, error) {
	filenames, err := filepath.Glob("/proc/*/statm")
	if err != nil {
		return nil, err
	}

	procCount := 0
	totalSize := int64(0)
	totalData := int64(0)
	totalShared := int64(0)
	totalResident := int64(0)

	for _, filename := range filenames {
		contents, err := ioutil.ReadFile(filename)
		if err != nil {
			log.Printf("Could not read %s for memory stats, skipping...", filename)
			continue
		}

		str_contents := string(contents)
		fields := strings.Split(str_contents, " ")

		// from the /proc docs
		// 		size       (1) total program size
		// 		(same as VmSize in /proc/[pid]/status)
		// resident   (2) resident set size
		// 		(same as VmRSS in /proc/[pid]/status)
		// shared     (3) number of resident shared pages (i.e., backed by a file)
		// 		(same as RssFile+RssShmem in /proc/[pid]/status)
		// text       (4) text (code)
		// lib        (5) library (unused since Linux 2.6; always 0)
		// data       (6) data + stack
		// dt         (7) dirty pages (unused since Linux 2.6; always 0)
		size, err := strconv.ParseInt(fields[0], 10, 64)
		var resident int64
		var shared int64
		// var text int64
		var data int64
		if err == nil {
			resident, err = strconv.ParseInt(fields[1], 10, 64)
		}
		if err == nil {
			shared, err = strconv.ParseInt(fields[2], 10, 64)
		}
		// if err == nil {
		// 	text, err = strconv.ParseInt(fields[3], 10, 64)
		// }
		if err == nil {
			data, err = strconv.ParseInt(fields[5], 10, 64)
		}

		if err != nil {
			log.Printf("Could not parse statm: %s", err)
			continue
		}

		procCount++
		totalSize += size
		totalData += data
		totalShared += shared
		totalResident += resident
	}

	return &MemoryUsage{totalSize, totalData, totalShared, totalResident, procCount}, nil
}

func (m *Monitor) GetProcessStatus(ctx context.Context, in *pb.GetProcessStatusRequest) (*pb.GetProcessStatusReply, error) {
	mem, err := getMemoryUsage()
	if err != nil {
		return nil, err
	}

	return &pb.GetProcessStatusReply{TotalMemory: mem.totalSize * PAGE_SIZE,
		TotalData:     mem.totalData * PAGE_SIZE,
		TotalShared:   mem.totalShared * PAGE_SIZE,
		TotalResident: mem.totalResident * PAGE_SIZE,
		ProcessCount:  int32(mem.procCount)}, nil
}

func (m *Monitor) ReadOutput(ctx context.Context, in *pb.ReadOutputRequest) (*pb.ReadOutputReply, error) {
	knownTaskIds := make([]string, 0, 100)
	m.mutex.Lock()
	stdoutPath, ok := m.logPerTaskId[in.TaskId]
	for _, taskId := range m.logPerTaskId {
		knownTaskIds = append(knownTaskIds, taskId)
	}
	m.mutex.Unlock()

	if !ok {
		return nil, fmt.Errorf("unknown task: %s (Known tasks: %s)", in.TaskId, strings.Join(knownTaskIds, ", "))
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
	log.Printf("StartWatchingLog(\"%s\", \"%s\")", taskId, stdoutPath)

	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.logPerTaskId[taskId] = stdoutPath
}
