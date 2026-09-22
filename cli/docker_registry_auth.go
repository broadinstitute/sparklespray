package sprinkles

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
)

// defaultDockerRegistryHost is always included in every configure-docker
// call, even the very first one. GCP Batch configures Docker's GCR/Artifact
// Registry auth automatically when it runs a container natively; this
// worker instead shells out to `docker run` itself (see
// executeDockerCommand), so it has to reproduce that setup by hand, and
// gcr.io is the one registry host worth covering pre-emptively.
const defaultDockerRegistryHost = "gcr.io"

// dockerRegistryAuth tracks which container-registry hostnames Docker has
// already been configured to authenticate against (via
// docker-credential-gcr), so configure-docker is only re-run when a task's
// image introduces a hostname not covered by a previous call -- not on
// every task. Safe for concurrent use.
type dockerRegistryAuth struct {
	mu    sync.Mutex
	hosts map[string]struct{} // hosts included in the last successful configure-docker call
	run   func(args ...string) ([]byte, error)
}

func newDockerRegistryAuth() *dockerRegistryAuth {
	return &dockerRegistryAuth{
		hosts: make(map[string]struct{}),
		run: func(args ...string) ([]byte, error) {
			cmd := exec.Command(dockerCredentialGCRExecutable, args...)
			// docker-credential-gcr's configure-docker verifies it can find
			// itself via PATH -- since that's how docker itself will invoke
			// credential helpers later -- so it needs the same augmented
			// PATH executeDockerCommand gives `docker run`, not just its own
			// absolute path as argv[0].
			cmd.Env = dockerRunEnv()
			return cmd.CombinedOutput()
		},
	}
}

// defaultDockerRegistryAuth is the process-lifetime cache used by
// executeDockerCommand.
var defaultDockerRegistryAuth = newDockerRegistryAuth()

// ensure makes sure docker-credential-gcr has configured Docker to
// authenticate against host, running configure-docker only if host isn't
// already covered by a previous call. It's a no-op for "" (a bare/Docker
// Hub image with no explicit registry) or a host docker-credential-gcr
// can't authenticate (see isGoogleContainerRegistryHost). On failure, the
// cache is left unchanged so a later call for the same host retries rather
// than silently treating it as configured.
func (d *dockerRegistryAuth) ensure(host string) error {
	if host == "" || !isGoogleContainerRegistryHost(host) {
		return nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.hosts[host]; ok {
		return nil
	}

	next := make(map[string]struct{}, len(d.hosts)+2)
	for h := range d.hosts {
		next[h] = struct{}{}
	}
	next[defaultDockerRegistryHost] = struct{}{}
	next[host] = struct{}{}

	hosts := make([]string, 0, len(next))
	for h := range next {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)

	args := []string{"configure-docker", "--registries=" + strings.Join(hosts, ",")}
	log.Printf("Configuring docker registry auth: %s %s", dockerCredentialGCRExecutable, strings.Join(args, " "))
	if out, err := d.run(args...); err != nil {
		return fmt.Errorf("docker-credential-gcr configure-docker: %w: %s", err, out)
	}

	d.hosts = next
	return nil
}

// registryHostFromImage extracts the registry hostname from a docker image
// reference, e.g. "us-west1-docker.pkg.dev/project/repo/image:tag" ->
// "us-west1-docker.pkg.dev". Returns "" for a bare/Docker-Hub image with no
// explicit registry, e.g. "ubuntu:latest" or "library/ubuntu".
//
// This follows the same convention Docker's own reference parser uses: the
// first "/"-separated component is a registry host only if it contains a
// "." or ":", or is exactly "localhost".
func registryHostFromImage(image string) string {
	first, _, ok := strings.Cut(image, "/")
	if !ok {
		return ""
	}
	if first == "localhost" || strings.ContainsAny(first, ".:") {
		return first
	}
	return ""
}

// isGoogleContainerRegistryHost reports whether host is a GCR or Artifact
// Registry hostname that docker-credential-gcr knows how to authenticate.
func isGoogleContainerRegistryHost(host string) bool {
	return host == defaultDockerRegistryHost ||
		strings.HasSuffix(host, ".gcr.io") ||
		strings.HasSuffix(host, "-docker.pkg.dev")
}

// dockerRunEnv returns the environment `docker run` should be spawned with:
// the current process's environment, with the docker binary's own directory
// (see dockerExecutable) guaranteed to be on PATH. When docker implicitly
// pulls a missing image, it looks up the docker-credential-gcr helper (set
// up by ensure, above) via PATH -- which isn't necessarily inherited from
// whatever minimal environment launched the worker process -- so without
// this, the credential helper can't be found and the pull fails.
func dockerRunEnv() []string {
	dirs := []string{filepath.Dir(dockerExecutable), filepath.Dir(dockerCredentialGCRExecutable)}

	env := os.Environ()
	foundPath := false
	for i, kv := range env {
		rest, ok := strings.CutPrefix(kv, "PATH=")
		if !ok {
			continue
		}
		foundPath = true
		for _, dir := range dirs {
			if pathListContains(rest, dir) {
				continue
			}
			rest = rest + string(os.PathListSeparator) + dir
		}
		env[i] = "PATH=" + rest
	}
	if !foundPath {
		env = append(env, "PATH="+strings.Join(dirs, string(os.PathListSeparator)))
	}
	return env
}

// pathListContains reports whether dir is one of the entries of path (a
// PATH-style, os.PathListSeparator-joined list).
func pathListContains(path, dir string) bool {
	return slices.Contains(filepath.SplitList(path), dir)
}
