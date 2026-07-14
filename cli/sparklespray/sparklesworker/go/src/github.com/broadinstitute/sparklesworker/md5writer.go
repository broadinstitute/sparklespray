package sparklesworker

import (
	"crypto/md5"
	"hash"
	"io"
)

type MD5Writer struct {
	writer io.Writer
	hash   hash.Hash
}

func NewMD5Writer(w io.Writer) *MD5Writer {
	return &MD5Writer{
		writer: w,
		hash:   md5.New(),
	}
}

func (w *MD5Writer) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	if err == nil {
		w.hash.Write(p[:n])
	}
	return
}

func (w *MD5Writer) MD5() []byte {
	return w.hash.Sum(nil)
}
