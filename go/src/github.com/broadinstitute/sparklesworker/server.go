package sparklesworker

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

	"github.com/broadinstitute/sparklesworker/pb"
)

type Monitor struct {
	pb.UnimplementedMonitorServer
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

// CPUStats holds CPU time counters from /proc/stat (in jiffies)
type CPUStats struct {
	User   int64
	System int64
	Idle   int64
	Iowait int64
}

// getCPUStats reads CPU counters from /proc/stat
func getCPUStats() (*CPUStats, error) {
	contents, err := ioutil.ReadFile("/proc/stat")
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "cpu ") {
			fields := strings.Fields(line)
			if len(fields) < 8 {
				return nil, fmt.Errorf("unexpected /proc/stat format")
			}
			// fields: cpu user nice system idle iowait irq softirq ...
			user, _ := strconv.ParseInt(fields[1], 10, 64)
			nice, _ := strconv.ParseInt(fields[2], 10, 64)
			system, _ := strconv.ParseInt(fields[3], 10, 64)
			idle, _ := strconv.ParseInt(fields[4], 10, 64)
			iowait, _ := strconv.ParseInt(fields[5], 10, 64)

			return &CPUStats{
				User:   user + nice, // combine user and nice
				System: system,
				Idle:   idle,
				Iowait: iowait,
			}, nil
		}
	}
	return nil, fmt.Errorf("cpu line not found in /proc/stat")
}

// SystemMemory holds system-wide memory info from /proc/meminfo (in bytes)
type SystemMemory struct {
	Total     int64
	Available int64
	Free      int64
}

// getSystemMemory reads memory info from /proc/meminfo
func getSystemMemory() (*SystemMemory, error) {
	contents, err := ioutil.ReadFile("/proc/meminfo")
	if err != nil {
		return nil, err
	}

	mem := &SystemMemory{}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, _ := strconv.ParseInt(fields[1], 10, 64)
		value *= 1024 // convert from kB to bytes

		switch fields[0] {
		case "MemTotal:":
			mem.Total = value
		case "MemAvailable:":
			mem.Available = value
		case "MemFree:":
			mem.Free = value
		}
	}
	return mem, nil
}

// MemoryPressure holds PSI metrics from /proc/pressure/memory
type MemoryPressure struct {
	SomeAvg10 int32 // percentage * 100, or -1 if unavailable
	FullAvg10 int32
}

// getMemoryPressure reads PSI memory pressure (gracefully returns -1 if unavailable)
func getMemoryPressure() *MemoryPressure {
	contents, err := ioutil.ReadFile("/proc/pressure/memory")
	if err != nil {
		// PSI not available (older kernel or not enabled)
		return &MemoryPressure{SomeAvg10: -1, FullAvg10: -1}
	}

	pressure := &MemoryPressure{SomeAvg10: -1, FullAvg10: -1}
	lines := strings.Split(string(contents), "\n")
	for _, line := range lines {
		// Format: some avg10=0.00 avg60=0.00 avg300=0.00 total=0
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		var target *int32
		if fields[0] == "some" {
			target = &pressure.SomeAvg10
		} else if fields[0] == "full" {
			target = &pressure.FullAvg10
		} else {
			continue
		}

		// Parse avg10=X.XX
		for _, field := range fields[1:] {
			if strings.HasPrefix(field, "avg10=") {
				valStr := strings.TrimPrefix(field, "avg10=")
				val, err := strconv.ParseFloat(valStr, 64)
				if err == nil {
					*target = int32(val * 100) // convert to percentage * 100
				}
				break
			}
		}
	}
	return pressure
}

func (m *Monitor) GetProcessStatus(ctx context.Context, in *pb.GetProcessStatusRequest) (*pb.GetProcessStatusReply, error) {
	mem, err := getMemoryUsage()
	if err != nil {
		return nil, err
	}

	reply := &pb.GetProcessStatusReply{
		TotalMemory:   mem.totalSize * PAGE_SIZE,
		TotalData:     mem.totalData * PAGE_SIZE,
		TotalShared:   mem.totalShared * PAGE_SIZE,
		TotalResident: mem.totalResident * PAGE_SIZE,
		ProcessCount:  int32(mem.procCount),
	}

	// Add CPU stats (best effort - don't fail if unavailable)
	if cpu, err := getCPUStats(); err == nil {
		reply.CpuUser = cpu.User
		reply.CpuSystem = cpu.System
		reply.CpuIdle = cpu.Idle
		reply.CpuIowait = cpu.Iowait
	}

	// Add system memory info (best effort)
	if sysMem, err := getSystemMemory(); err == nil {
		reply.MemTotal = sysMem.Total
		reply.MemAvailable = sysMem.Available
		reply.MemFree = sysMem.Free
	}

	// Add memory pressure (gracefully handles missing PSI)
	pressure := getMemoryPressure()
	reply.MemPressureSomeAvg10 = pressure.SomeAvg10
	reply.MemPressureFullAvg10 = pressure.FullAvg10

	return reply, nil
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

	//	log.Printf("reading %d bytes from %s (offset %d)", in.Size, stdoutPath, in.Offset)
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
