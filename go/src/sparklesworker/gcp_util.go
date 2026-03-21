package sparklesworker

import (
	"errors"
	"io/ioutil"
	"log"
	"net/http"
)

func getMetadata(url string) (string, error) {
	var client http.Client
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Printf("Got status=%d from fetching %s", resp.StatusCode, url)
		return "", errors.New("fetching metadata failed")
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	return string(bodyBytes), err
}

func GetExternalIP() (string, error) {
	return getMetadata("http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip")
}

func GetInstanceName() (string, error) {
	return getMetadata("http://metadata.google.internal/computeMetadata/v1/instance/name")
}

func GetInstanceZone() (string, error) {
	return getMetadata("http://metadata.google.internal/computeMetadata/v1/instance/zone")
}
