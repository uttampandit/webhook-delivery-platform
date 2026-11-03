package api

import (
	"encoding/json"
	"net/http"
)

type errorCode string

const (
	codeUnauthorized          errorCode = "unauthorized"
	codeInvalidRequest        errorCode = "invalid_request"
	codeNotFound              errorCode = "not_found"
	codeDependencyUnavailable errorCode = "dependency_unavailable"
	codeInternalError         errorCode = "internal_error"
)

type errorEnvelope struct {
	Error errorDetails `json:"error"`
}

type errorDetails struct {
	Code      errorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"request_id"`
}

func writeError(w http.ResponseWriter, r *http.Request, code errorCode) {
	status, message := errorDetailsFor(code)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{
		Error: errorDetails{Code: code, Message: message, RequestID: requestIDFrom(r)},
	})
}

func errorDetailsFor(code errorCode) (int, string) {
	switch code {
	case codeUnauthorized:
		return http.StatusUnauthorized, "unauthorized"
	case codeInvalidRequest:
		return http.StatusBadRequest, "invalid request"
	case codeNotFound:
		return http.StatusNotFound, "not found"
	case codeDependencyUnavailable:
		return http.StatusServiceUnavailable, "dependency unavailable"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}

func methodNotAllowed(w http.ResponseWriter, allow string) {
	w.Header().Set("Allow", allow)
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}
