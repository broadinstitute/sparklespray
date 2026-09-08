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
//
// prefix is the path segment (e.g. "/sparkles", or "" at the root) the
// caller mounts this handler under -- Handler expects to see paths already
// relative to that mount point (the caller strips it, e.g. with
// http.StripPrefix), but needs prefix itself to inject a matching
// "<base href>" into index.html so the SPA's relative asset URLs and
// react-router's basename resolve correctly regardless of where it's
// mounted.
func Handler(prefix string) http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		// Only possible if the embed directive/directory layout is broken --
		// a build-time programmer error, not a runtime condition.
		panic(fmt.Sprintf("webui: %v", err))
	}

	index := loadIndexHTML(sub, prefix)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqPath := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if reqPath == "" || reqPath == "." {
			reqPath = "index.html"
		}

		if reqPath != "index.html" {
			if info, err := fs.Stat(sub, reqPath); err == nil && !info.IsDir() {
				setCacheControl(w, reqPath)
				http.ServeFileFS(w, r, sub, reqPath)
				return
			}

			// Not a real file. If it looks like a missed static asset (the
			// last path segment has a file extension, e.g. /assets/foo.js,
			// /favicon.ico), 404 rather than masking the failure with an
			// HTML response -- the browser would otherwise try to parse
			// index.html as JS/CSS and fail confusingly.
			if strings.Contains(path.Base(reqPath), ".") {
				http.NotFound(w, r)
				return
			}
		}

		// Either a direct hit on index.html, or (having fallen through the
		// checks above) a client-side route (e.g. /jobs/123) -- serve the
		// prefix-adjusted index.html either way.
		setCacheControl(w, "index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(index)
	})
}

// loadIndexHTML reads dist/index.html and injects a "<base href>" tag
// matching prefix, so the page's relative asset URLs (Vite is configured
// with base: "./") resolve under prefix regardless of how deep the current
// client-side route is, and so the frontend can read prefix back at runtime
// (via document.querySelector("base")) to configure react-router's basename
// and prefix its own API requests.
func loadIndexHTML(sub fs.FS, prefix string) []byte {
	data, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		// Same class of error as the fs.Sub check above: a broken build, not
		// a runtime condition.
		panic(fmt.Sprintf("webui: reading index.html: %v", err))
	}

	base := fmt.Sprintf(`<base href="%s/">`, prefix)
	html := string(data)
	if i := strings.Index(html, "<head>"); i != -1 {
		insertAt := i + len("<head>")
		html = html[:insertAt] + base + html[insertAt:]
	} else {
		// No <head> tag found (unexpected for the Vite build output) --
		// prepend so the app still gets a base href rather than silently
		// serving unprefixed URLs.
		html = base + html
	}
	return []byte(html)
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
