// Package webui embeds the built dashboard frontend (dashboard/dist, copied
// in by build.sh before compilation) and serves it as a static
// site with an SPA fallback, so the dashboard-backend binary can serve the
// UI itself without a separate web server or deploy step.
package webui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded frontend build:
// real files under dist/ are served as-is; any other path that doesn't look
// like a missed static-asset request falls back to index.html, so
// client-side routing (react-router's BrowserRouter) works on deep links and
// page refreshes.
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		// Only possible if the embed directive/directory layout is broken --
		// a build-time programmer error, not a runtime condition.
		panic(fmt.Sprintf("webui: %v", err))
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if reqPath == "" || reqPath == "." {
			reqPath = "index.html"
		}

		if info, err := fs.Stat(sub, reqPath); err == nil && !info.IsDir() {
			setCacheControl(w, reqPath)
			http.ServeFileFS(w, r, sub, reqPath)
			return
		}

		// Not a real file. If it looks like a missed static asset (the last
		// path segment has a file extension, e.g. /assets/foo.js,
		// /favicon.ico), 404 rather than masking the failure with an HTML
		// response -- the browser would otherwise try to parse index.html as
		// JS/CSS and fail confusingly.
		if strings.Contains(path.Base(reqPath), ".") {
			http.NotFound(w, r)
			return
		}

		// Otherwise, assume it's a client-side route (e.g. /jobs/123) and
		// fall back to index.html.
		setCacheControl(w, "index.html")
		http.ServeFileFS(w, r, sub, "index.html")
	})
}

// setCacheControl sets a caching policy appropriate for the given embedded
// path: assets/ filenames are content-hashed by Vite, so they can be cached
// forever; everything else (index.html, favicon.ico, ...) has a stable URL
// across deploys and must always be revalidated.
func setCacheControl(w http.ResponseWriter, reqPath string) {
	if strings.HasPrefix(reqPath, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
}
