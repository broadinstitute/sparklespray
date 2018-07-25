package kubequeconsume

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

type IOClient interface {
	Upload(srcPath string, destURL string) error
	UploadBytes(destURL string, data []byte) error
	Download(srcURL string, destPath string) error
	DownloadAsBytes(srcURL string) ([]byte, error)
}

type GCSIOClient struct {
	ctx    context.Context
	client *storage.Client
}

func NewIOClient(ctx context.Context, tokenSource oauth2.TokenSource) (IOClient, error) {
	client, err := storage.NewClient(ctx, option.WithTokenSource(tokenSource))
	if err != nil {
		return nil, err
	}
	return &GCSIOClient{ctx: ctx, client: client}, nil
}

func (ioc *GCSIOClient) Upload(src string, destURL string) error {

	// log.Printf("Uploading %s -> %s", src, destURL)
	obj, err := ioc.getObj(destURL)
	// log.Printf("getObj(%s) -> %v, %v", destURL, obj, err)
	if err != nil {
		return err
	}

	r, err := os.Open(src)
	if err != nil {
		return err
	}
	rFi, err := r.Stat()
	if err != nil {
		return err
	}
	fileSize := rFi.Size()
	// log.Printf("Successfully opened src (size=%d)", fileSize)
	defer r.Close()

	w := obj.NewWriter(ioc.ctx)
	// log.Printf("Successfully opened dst")
	defer w.Close()

	var n int64
	if n, err = io.Copy(NotifyOnWrite(w), r); err != nil {
		return err
	}

	// if n != fileSize {
	// 	return fmt.Errorf("While uploading %s to %s, only %d out of %d bytes transfered", src, destURL, n, fileSize)
	// }

	attrs, err := obj.Attrs(ioc.ctx)
	if err != nil {
		return err
	}
	if n != fileSize {
		return fmt.Errorf("While uploading %s to %s, expected size to be %d, but saw %d bytes transfered", src, destURL, n, attrs.Size)
	}

	// log.Printf("Successfully copied")

	return nil
}

func (ioc *GCSIOClient) UploadBytes(destURL string, data []byte) error {
	obj, err := ioc.getObj(destURL)
	if err != nil {
		return err
	}

	w := obj.NewWriter(ioc.ctx)
	defer w.Close()

	_, err = w.Write(data)
	return err
}

func (ioc *GCSIOClient) getObj(srcUrl string) (*storage.ObjectHandle, error) {
	urlPattern := regexp.MustCompile("^gs://([^/]+)/(.+)$")

	groups := urlPattern.FindStringSubmatch(srcUrl)
	if groups == nil {
		return nil, errors.New("invalid url: " + srcUrl)
	}
	bucketName := groups[1]
	keyName := groups[2]

	bucket := ioc.client.Bucket(bucketName)
	obj := bucket.Object(keyName)

	return obj, nil
}

func (ioc *GCSIOClient) DownloadAsBytes(srcUrl string) ([]byte, error) {
	obj, err := ioc.getObj(srcUrl)
	if err != nil {
		return nil, err
	}

	r, err := obj.NewReader(ioc.ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (ioc *GCSIOClient) Download(srcUrl string, destPath string) error {
	log.Printf("Downloading %s -> %s\n", srcUrl, destPath)

	obj, err := ioc.getObj(srcUrl)
	if err != nil {
		return err
	}

	parentDir := path.Dir(destPath)
	os.MkdirAll(parentDir, os.ModePerm)

	tf, err := ioutil.TempFile(parentDir, "downloading")
	if err != nil {
		return err
	}
	defer tf.Close()
	tmpDestPath := tf.Name()

	w, err := os.OpenFile(tmpDestPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0700)
	if err != nil {
		return err
	}
	defer w.Close()

	r, err := obj.NewReader(ioc.ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	if _, err := io.Copy(NotifyOnWrite(w), r); err != nil {
		return err
	}

	err = os.Rename(tmpDestPath, destPath)
	if err != nil {
		return err
	}

	return nil
}

func GetInstanceName() (string, error) {
	url := "http://metadata.google.internal/computeMetadata/v1/instance/name"
	var client http.Client
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Got status=%d from fetching instance name", resp.StatusCode)
		return "", errors.New("fetching instance name failed")
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	return string(bodyBytes), err
}

func GetInstanceZone() (string, error) {
	url := "http://metadata.google.internal/computeMetadata/v1/instance/zone"
	var client http.Client
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Got status=%d from fetching instance zone", resp.StatusCode)
		return "", errors.New("fetching instance zone failed")
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	return string(bodyBytes), err
}
