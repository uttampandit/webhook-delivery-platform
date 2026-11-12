package api

import (
	"io/fs"
	"net/http"
	"path"
	"strings"
)

// WithStaticUI serves built dashboard assets and leaves API/health routes with
// the API handler. A missing asset falls back to index.html for browser routes.
func WithStaticUI(apiHandler http.Handler, files fs.FS) http.Handler {
	if apiHandler == nil || files == nil {
		return apiHandler
	}
	static := http.FileServer(http.FS(files))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isAPIPath(r.URL.Path) {
			apiHandler.ServeHTTP(w, r)
			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" {
			name = "index.html"
		}
		if fs.ValidPath(name) {
			if entry, err := fs.Stat(files, name); err == nil && !entry.IsDir() {
				static.ServeHTTP(w, r)
				return
			}
		}

		if acceptsHTML(r) && path.Ext(r.URL.Path) == "" {
			if _, err := fs.Stat(files, "index.html"); err == nil {
				page := r.Clone(r.Context())
				page.URL.Path = "/"
				page.URL.RawPath = ""
				static.ServeHTTP(w, page)
				return
			}
		}
		http.NotFound(w, r)
	})
}

func isAPIPath(value string) bool {
	return value == "/v1" || strings.HasPrefix(value, "/v1/") ||
		value == "/health/live" || value == "/health/ready" || value == "/metrics"
}

func acceptsHTML(r *http.Request) bool {
	for _, value := range r.Header.Values("Accept") {
		for _, mediaType := range strings.Split(value, ",") {
			mediaType = strings.TrimSpace(strings.SplitN(mediaType, ";", 2)[0])
			if mediaType == "text/html" || mediaType == "application/xhtml+xml" {
				return true
			}
		}
	}
	return false
}
