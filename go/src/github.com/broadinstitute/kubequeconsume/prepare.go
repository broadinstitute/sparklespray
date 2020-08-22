package kubequeconsume

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
	"time"
)

var GCSFuseMountOptions = []string{"--foreground", "-o", "ro",
	"--stat-cache-ttl", "24h", "--type-cache-ttl", "24h",
	"--file-mode", "555", "--implicit-dirs"}

var whitespaceRegExp = regexp.MustCompile("\\s")

func getActiveMountpoints() map[string]bool {
	mountPoints := make(map[string]bool)
	mountsBytes, err := ioutil.ReadFile("/proc/mounts")
	if err != nil {
		log.Fatalf("Could not read /proc/mounts: %v", err)
	}
	mounts := strings.Split(string(mountsBytes), "\n")
	log.Printf("Mounts: %s", mounts)
	for _, mount := range mounts {
		fields := whitespaceRegExp.Split(mount, -1)
		if len(fields) > 1 {
			//log.Printf("Found mount: %s", fields[1])
			mountPoints[fields[1]] = true
		}
	}
	return mountPoints
}

func waitForMounts(bucketDirs []string) {
	// how do we tell when the buckets have successfully mounted?
	attempt := 0
	for {
		if attempt > 60 {
			log.Fatalf("Giving up waiting for mounts")
		}
		attempt++

		mountPoints := getActiveMountpoints()
		allFound := true
		for _, bucketDir := range bucketDirs {
			if !mountPoints[bucketDir] {
				allFound = false
				log.Printf("Mount point %s not present", bucketDir)
				break
			}
		}

		if allFound {
			break
		}

		log.Printf("Sleeping...")
		time.Sleep(1 * time.Second)
	}
}

func Prepare(gcsfuseExecutable string, prepBucketDir string, buckets []string, injectConsumeExe string) error {
	log.Printf("Prepare started")

	// Do the bucket mounts
	log.Printf("Preparing %d bucket mounts", len(buckets))
	bucketDirs := make([]string, 0, 100)
	for _, bucket := range buckets {
		bucketDir := path.Join(prepBucketDir, bucket)
		bucketDirs = append(bucketDirs, bucketDir)

		if _, err := os.Stat(bucketDir); os.IsNotExist(err) {
			os.MkdirAll(bucketDir, 0766)
		}

		exeAsSlice := []string{gcsfuseExecutable}
		command := append(exeAsSlice, GCSFuseMountOptions...)
		command = append(command, bucket, bucketDir)
		log.Printf("Mounting bucket: %v", command)
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout

		mountInBackground := func() {
			err := cmd.Run()
			if err != nil {
				log.Fatalf("Mount of %s exited: %s", bucket, err)
			}
			log.Printf("Mount of %s exited cleanly", bucket)
		}
		go mountInBackground()
	}

	// wait for buckets to mount
	waitForMounts(bucketDirs)

	// copy this executable into a path accessible by the 2nd container
	// this is used to also signal that all the mounting is done
	if injectConsumeExe != "" {
		log.Printf("copying helper to %s", injectConsumeExe)
		parentDir := path.Dir(injectConsumeExe)
		if _, err := os.Stat(parentDir); os.IsNotExist(err) {
			err = os.MkdirAll(parentDir, 0766)
			if err != nil {
				return err
			}
		}
		// do this by copying and moving to ensure the appearance of the file is atomic
		injectConsumeExeTmp := injectConsumeExe + ".tmp"
		err := copyFile(os.Args[0], injectConsumeExeTmp)
		if err != nil {
			return err
		}

		err = os.Chmod(injectConsumeExeTmp, 0755)
		if err != nil {
			return err
		}

		err = os.Rename(injectConsumeExeTmp, injectConsumeExe)
		if err != nil {
			return err
		}
	}

	log.Printf("Prepare complete")
	if len(buckets) > 0 {
		log.Printf("Mounted buckets so sleeping forever to ensure gcsfuse continues to run")
		for {
			time.Sleep(time.Hour)
		}
	}
	return nil
}
