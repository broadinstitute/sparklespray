package kubequeconsume

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
)

var GCSFuseMountOptions = []string{"--foreground", "-o", "ro",
	"--stat-cache-ttl", "24h", "--type-cache-ttl", "24h",
	"--file-mode", "755", "--implicit-dirs"}

func Prepare(gcsfuseExecutable string, prepBucketDir string, buckets []string, injectConsumeExe string) {
	// copy this executable into a path accessible by the 2nd container
	if injectConsumeExe != "" {
		parentDir := path.Dir(injectConsumeExe)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			os.MkdirAll(parentDir, 0766)
		}
		copyFile(os.Args[0], injectConsumeExe)
		os.Chmod(injectConsumeExe, 0755)
	}

	// now do the bucket mounts
	for _, bucket := range buckets {
		bucketDir := path.Join(prepBucketDir, bucket)
		if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
			os.MkdirAll(bucketDir, 0766)
		}

		exeAsSlice := []string{gcsfuseExecutable}
		command := append(exeAsSlice, GCSFuseMountOptions...)
		command = append(command, bucket, fmt.Sprintf("%s/%s", prepBucketDir, bucket))
		log.Printf("Mounting bucket: %v", command)
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		err := cmd.Start()
		if err != nil {
			log.Fatal(err)
		}
	}
}

// func copyFile(src, dst string) error {
// 	srcFile, err := os.Open(src)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer srcFile.Close()

// 	dstFile, err := os.Create(dst)
// 	if err != nil {
// 		return 0, err
// 	}
// 	defer dstFile.Close()
// 	_, err := io.Copy(dstFile, srcFile)
// 	return err
// }
