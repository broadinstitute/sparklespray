package copyexe

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/urfave/cli"
)

var CopyexeCmd = cli.Command{
	Name:  "copyexe",
	Usage: "Copy the worker executable to a destination path",
	Flags: []cli.Flag{
		cli.StringFlag{Name: "dst", Usage: "Destination path for the copied executable"},
	},
	Action: copyexe,
}

func copyexe(c *cli.Context) error {
	dst := c.String("dst")
	executablePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("couldn't get path to executable: %s", err)
	}

	log.Printf("Installing (copying %s to %s)", executablePath, dst)

	parentDir := path.Dir(dst)
	os.MkdirAll(parentDir, 0777)

	reader, err := os.Open(executablePath)
	if err != nil {
		return fmt.Errorf("could open %s for reading: %s", executablePath, err)
	}
	defer reader.Close()

	writer, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	if err != nil {
		return fmt.Errorf("could open %s for writing: %s", dst, err)
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return fmt.Errorf("failed copying %s to %s writing: %s", executablePath, dst, err)
	}

	log.Printf("Installed %s to %s", executablePath, dst)

	return nil
}
