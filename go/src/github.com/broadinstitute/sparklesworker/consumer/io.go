package consumer

// IOClient defines the interface for I/O operations (GCS uploads/downloads)
type IOClient interface {
	Upload(srcPath string, destURL string) error
	UploadBytes(destURL string, data []byte) error
	Download(srcURL string, destPath string) error
	DownloadAsBytes(srcURL string) ([]byte, error)
	IsExists(url string) (bool, error)
}
