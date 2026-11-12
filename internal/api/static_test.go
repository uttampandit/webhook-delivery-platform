package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestStaticUIAndAPIUseSameOriginWithoutHidingAPIRoutes(t *testing.T) {
	files := fstest.MapFS{
		"index.html":       &fstest.MapFile{Data: []byte("<main>dashboard shell</main>")},
		"assets/app.js":    &fstest.MapFile{Data: []byte("window.dashboardReady = true")},
		"assets/style.css": &fstest.MapFile{Data: []byte("body { color: black; }")},
	}
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"api route"}`))
	})
	handler := WithStaticUI(apiHandler, files)

	for _, test := range []struct {
		path, accept, want string
		status             int
	}{
		{path: "/", want: "dashboard shell", status: http.StatusOK},
		{path: "/events", accept: "text/html", want: "dashboard shell", status: http.StatusOK},
		{path: "/assets/app.js", want: "dashboardReady", status: http.StatusOK},
		{path: "/missing.js", accept: "text/html", status: http.StatusNotFound},
		{path: "/v1/not-found", accept: "text/html", want: "api route", status: http.StatusNotFound},
		{path: "/health/ready", accept: "text/html", want: "api route", status: http.StatusNotFound},
	} {
		t.Run(test.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, test.path, nil)
			if test.accept != "" {
				request.Header.Set("Accept", test.accept)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status || (test.want != "" && !strings.Contains(recorder.Body.String(), test.want)) {
				t.Fatalf("GET %s = %d %q, want %d containing %q", test.path, recorder.Code, recorder.Body.String(), test.status, test.want)
			}
		})
	}
}

func TestStaticUIDisabledWithoutFiles(t *testing.T) {
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	if got := WithStaticUI(apiHandler, nil); got == nil {
		t.Fatal("nil static filesystem removed API handler")
	}
	if got := WithStaticUI(nil, fstest.MapFS{}); got != nil {
		t.Fatal("nil API handler should remain nil")
	}
}
