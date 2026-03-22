package fetch

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"cloud.google.com/go/storage"
	"github.com/urfave/cli"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

var FetchCmd = cli.Command{
	Name:  "fetch",
	Usage: "Download a file from Google Cloud Storage with MD5 verification",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "expectMD5", Usage: "Expected MD5 hash (hex-encoded) of the downloaded file"},
		cli.StringFlag{Name: "src", Usage: "Source GCS path (gs://bucket/object)"},
		cli.StringFlag{Name: "dst", Usage: "Destination local file path"},
	},
	Action: fetch,
}

func fetch(c *cli.Context) error {
	log.Printf("Starting fetch")

	expectMD5 := c.String("expectMD5")
	src := c.String("src")
	dst := c.String("dst")

	ctx := context.Background()

	httpClient, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/compute.readonly")
	if err != nil {
		log.Printf("Could not create default client: %v", err)
		return err
	}

	client, err := storage.NewClient(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("could not get storage client: %s", err)
	}

	bucketName, objectName := splitGCSPath(src)
	if bucketName == "" {
		return fmt.Errorf("expected source to be gs://<bucket>/<object> but source was \"%s\"", src)
	}

	object := client.Bucket(bucketName).Object(objectName)
	reader, err := object.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("could not open %s for reading: %s", src, err)
	}

	err = CopyToFile(reader, dst, expectMD5)
	if err != nil {
		return fmt.Errorf("copy of %s failed: %s", src, err)
	}

	return nil
}

func splitGCSPath(p string) (bucket, key string) {
	re := regexp.MustCompile(`^gs://([^/]+)/(.+)$`)
	matches := re.FindStringSubmatch(p)
	if len(matches) == 3 {
		return matches[1], matches[2]
	}
	return "", ""
}

func CopyToFile(src io.Reader, dstPath string, expectedMD5 string) error {
	dir := filepath.Dir(dstPath)
	tmpFile, err := os.CreateTemp(dir, "*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	success := false
	defer func() {
		tmpFile.Close()
		if !success {
			os.Remove(tmpPath)
		}
	}()

	wrapper := newMD5Writer(tmpFile)

	if _, err := io.Copy(wrapper, src); err != nil {
		return fmt.Errorf("copying data: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	md5sum := hex.EncodeToString(wrapper.md5())
	if md5sum != expectedMD5 {
		return fmt.Errorf("MD5 hash did not match expected")
	}

	if err := os.Rename(tmpPath, dstPath); err != nil {
		return fmt.Errorf("renaming temp file: %w", err)
	}

	success = true
	return nil
}

type md5Writer struct {
	writer io.Writer
	hash   hash.Hash
}

func newMD5Writer(w io.Writer) *md5Writer {
	return &md5Writer{
		writer: w,
		hash:   md5.New(),
	}
}

func (w *md5Writer) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	if err == nil {
		w.hash.Write(p[:n])
	}
	return
}

func (w *md5Writer) md5() []byte {
	return w.hash.Sum(nil)
}
