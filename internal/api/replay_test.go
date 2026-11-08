package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const replayDeliveryID = "11111111-1111-4111-8111-111111111111"

type replayTestRepository struct {
	testRepository
	result                 store.ReplayResult
	err                    error
	calls                  int
	tenant, id, key, actor string
	deadline               bool
}

func (r *replayTestRepository) Replay(ctx context.Context, tenant, id, key, actor string) (store.ReplayResult, error) {
	r.calls++
	r.tenant = tenant
	r.id = id
	r.key = key
	r.actor = actor
	_, r.deadline = ctx.Deadline()
	return r.result, r.err
}
func replayRequest(body, key string) *http.Request {
	r := httptest.NewRequest("POST", "/v1/deliveries/"+replayDeliveryID+"/replay", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testAPIToken)
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	r.Header.Set("X-Tenant-ID", "attacker")
	r.Header.Set("X-Actor", "attacker")
	return r
}
func TestReplayHTTPValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, key string
		status          int
		code            errorCode
	}{
		{"missing key", "{}", "", 400, codeInvalidRequest},
		{"bad key", "{}", "a b", 400, codeInvalidRequest},
		{"long key", "{}", strings.Repeat("x", 129), 400, codeInvalidRequest},
		{"empty", "", "k", 400, codeInvalidJSON},
		{"malformed", "{", "k", 400, codeInvalidJSON},
		{"trailing", "{} {}", "k", 400, codeInvalidJSON},
		{"null", "null", "k", 400, codeInvalidRequest},
		{"array", "[]", "k", 400, codeInvalidRequest},
		{"field", `{"actor":"x"}`, "k", 400, codeInvalidRequest},
		{"duplicate", `{"a":1,"a":2}`, "k", 400, codeInvalidRequest},
		{"oversize", strings.Repeat(" ", store.MaxEventRequestBytes) + "{}", "k", 413, codeBodyTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &replayTestRepository{}
			w := httptest.NewRecorder()
			NewHandler(testSettings(t), repo).ServeHTTP(w, replayRequest(tc.body, tc.key))
			assertCreateEventError(t, w, tc.status, tc.code)
			if repo.calls != 0 {
				t.Fatal("invalid input reached store")
			}
		})
	}
	for _, kind := range []string{"auth", "uuid", "malformed uuid", "method", "duplicate key", "extra path"} {
		t.Run(kind, func(t *testing.T) {
			repo := &replayTestRepository{}
			w := httptest.NewRecorder()
			r := replayRequest("{}", "k")
			want := 400
			switch kind {
			case "auth":
				r.Header.Del("Authorization")
				want = 401
			case "uuid":
				r.URL.Path = "/v1/deliveries/bad/replay"
			case "malformed uuid":
				r.URL.Path = "/v1/deliveries/" + strings.Repeat("-", 36) + "/replay"
			case "method":
				r.Method = "GET"
				want = 405
			case "duplicate key":
				r.Header.Add("Idempotency-Key", "k")
			case "extra path":
				r.URL.Path = "/v1/deliveries/extra/" + replayDeliveryID + "/replay"
				want = 404
			}
			NewHandler(testSettings(t), repo).ServeHTTP(w, r)
			if w.Code != want || repo.calls != 0 {
				t.Fatal(w.Code, repo.calls)
			}
			if want == 405 && w.Header().Get("Allow") != "POST" {
				t.Fatal("Allow")
			}
		})
	}
}
func TestReplayHTTPResponsesAndPrincipal(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		repo := &replayTestRepository{result: store.ReplayResult{ReplayID: replayDeliveryID, DeliveryID: replayDeliveryID, CycleNo: 2, Deduplicated: duplicate}}
		w := httptest.NewRecorder()
		NewHandler(testSettings(t), repo).ServeHTTP(w, replayRequest(" \n{ }\t", "key"))
		want := 202
		if duplicate {
			want = 200
		}
		if w.Code != want {
			t.Fatal(w.Code, w.Body.String())
		}
		var got map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if len(got) != 4 || got["cycle_no"] != float64(2) || got["deduplicated"] != duplicate || got["delivery_id"] != replayDeliveryID || got["replay_id"] != replayDeliveryID {
			t.Fatal(got)
		}
		if repo.tenant != config.TenantID || repo.actor != operatorActor || repo.id != replayDeliveryID || repo.key != "key" || !repo.deadline {
			t.Fatal("principal/deadline", repo)
		}
	}
	for _, tc := range []struct {
		err    error
		status int
		code   errorCode
	}{
		{store.ErrInvalidState, 409, codeInvalidState}, {store.ErrNotFound, 404, codeNotFound}, {store.ErrIdempotencyConflict, 409, codeIdempotencyConflict},
		{store.ErrStoreUnavailable, 503, codeDependencyUnavailable}, {store.ErrAmbiguousCommit, 503, codeDependencyUnavailable}, {store.ErrInvalidInput, 400, codeInvalidRequest}, {errors.New("secret SQL details"), 500, codeInternalError},
	} {
		repo := &replayTestRepository{err: tc.err}
		w := httptest.NewRecorder()
		NewHandler(testSettings(t), repo).ServeHTTP(w, replayRequest("{}", "k"))
		assertCreateEventError(t, w, tc.status, tc.code)
		if strings.Contains(w.Body.String(), "secret") {
			t.Fatal("leaked error")
		}
	}
}
