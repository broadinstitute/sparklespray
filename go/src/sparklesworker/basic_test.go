package sparklesworker

import (
	"log"
	"os"
	"syscall"
	"testing"
)

func TestBasic(t *testing.T) {
	exePath := "/bin/sh"
	args := []string{"/bin/sh", "-c", "echo hello"}
	stdout, err := os.Create("stdout.txt")
	log.Printf("err=%v", err)
	defer stdout.Close()
	attr := &os.ProcAttr{Dir: ".", Env: nil, Files: []*os.File{nil, stdout, stdout}}

	proc, err := os.StartProcess(exePath, args, attr)
	log.Printf("err=%v", err)

	procState, err2 := proc.Wait()
	log.Printf("err=%v", err2)

	status := procState.Sys().(syscall.WaitStatus)
	log.Printf("status=%v", status)

}
