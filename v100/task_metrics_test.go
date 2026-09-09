package v100

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetCPUCount(t *testing.T) {
	proc := t.TempDir()
	withRoots(t, t.TempDir(), proc)

	stat := `cpu  100 0 200 300 0 0 0 0 0 0
cpu0 25 0 50 75 0 0 0 0 0 0
cpu1 25 0 50 75 0 0 0 0 0 0
cpu2 25 0 50 75 0 0 0 0 0 0
cpu3 25 0 50 75 0 0 0 0 0 0
intr 12345
`
	if err := os.WriteFile(filepath.Join(proc, "stat"), []byte(stat), 0644); err != nil {
		t.Fatal(err)
	}

	count, err := getCPUCount()
	if err != nil {
		t.Fatalf("getCPUCount: %v", err)
	}
	if count != 4 {
		t.Errorf("count = %d, want 4", count)
	}
}

func TestGetCPUCountMissingFile(t *testing.T) {
	withRoots(t, t.TempDir(), t.TempDir())

	if _, err := getCPUCount(); err == nil {
		t.Fatal("expected an error when /proc/stat is missing")
	}
}
