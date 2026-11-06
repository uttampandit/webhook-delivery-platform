package api

import (
	"errors"
	"io"
	"net/http"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

var errRequestBodyTooLarge = errors.New("request body exceeds limit")

// readValidatedEventRequest applies a streaming body cap before delegating the
// strict JSON and DTO validation to the store's shared request parser.
func readValidatedEventRequest(w http.ResponseWriter, r *http.Request) (store.ValidatedEventRequest, error) {
	body := http.MaxBytesReader(w, r.Body, store.MaxEventRequestBytes)
	defer body.Close()

	raw, err := io.ReadAll(body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return store.ValidatedEventRequest{}, errRequestBodyTooLarge
		}
		return store.ValidatedEventRequest{}, store.ErrInvalidJSON
	}
	return store.ValidateEventRequest(raw)
}
