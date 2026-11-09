// Package democtl implements internal HTTP-only demo commands. It never opens
// a database or edits delivery state behind API/receiver contracts.
package democtl

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

type Endpoints struct {
	API      string
	Controls map[string]string
}

func DefaultEndpoints(mode config.RunMode) Endpoints {
	if mode == config.RunModeHost {
		return Endpoints{"http://127.0.0.1:8080", map[string]string{"fulfillment": "http://127.0.0.1:9092", "notification": "http://127.0.0.1:9093"}}
	}
	return Endpoints{"http://api:8080", map[string]string{"fulfillment": "http://fulfillment:9091", "notification": "http://notification:9091"}}
}

type Client struct {
	settings     config.Settings
	endpoints    Endpoints
	http         *http.Client
	PollInterval time.Duration
}

func New(s config.Settings, e Endpoints) *Client {
	return &Client{s, e, &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Proxy: nil, DisableKeepAlives: true, DisableCompression: true, MaxResponseHeaderBytes: 16 * 1024}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, 200 * time.Millisecond}
}

var errInput = errors.New("invalid command or flags")
var errResponse = errors.New("invalid server response")

func (c *Client) Run(ctx context.Context, args []string, out, stderr io.Writer) int {
	value, err := c.execute(ctx, args)
	// Do not echo flags, URLs, server bodies or raw transport errors. Defensively
	// redact even valid user-controlled output if it contains configured tokens.
	clean := func(s string) string {
		for _, token := range []string{c.settings.APIToken(), c.settings.DemoControlToken()} {
			if token != "" {
				s = strings.ReplaceAll(s, token, "[redacted]")
			}
		}
		return s
	}
	if err != nil {
		fmt.Fprintln(stderr, clean(err.Error()))
		return 1
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(stderr, "output encoding failed")
		return 1
	}
	fmt.Fprintln(out, clean(string(encoded)))
	return 0
}
func strictFlags(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	seen := map[string]bool{}
	for i := 0; i < len(args); i++ {
		token := args[i]
		if !strings.HasPrefix(token, "-") {
			return errInput
		}
		name, value, has := strings.Cut(strings.TrimLeft(token, "-"), "=")
		_ = value
		if name == "" || fs.Lookup(name) == nil || seen[name] {
			return errInput
		}
		seen[name] = true
		if !has {
			i++
			if i >= len(args) {
				return errInput
			}
		}
	}
	if fs.Parse(args) != nil || fs.NArg() != 0 {
		return errInput
	}
	return nil
}
func receivers(code string, allowBoth bool) ([]string, error) {
	if code == "both" && allowBoth {
		return []string{"fulfillment", "notification"}, nil
	}
	if code == "fulfillment" || code == "notification" {
		return []string{code}, nil
	}
	return nil, errInput
}
func uuid(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, ch := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if ch != '-' {
				return false
			}
		} else if !(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f') {
			return false
		}
	}
	return true
}
func validMode(mode string) bool {
	return mode == "normal" || mode == "http_503" || mode == "http_429" || mode == "commit_drop_once"
}
func (c *Client) request(ctx context.Context, method, url, token, key string, body []byte, out any) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, method, url, io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return errors.New("request configuration invalid")
	}
	r.ContentLength = int64(len(body))
	r.GetBody = nil
	r.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := c.http.Do(r)
	if err != nil {
		return errors.New("HTTP request failed")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("HTTP request rejected (status %d)", response.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return errResponse
	}
	if out != nil {
		if store.ValidateDemoJSON(raw) != nil || json.Unmarshal(raw, out) != nil {
			return errResponse
		}
	}
	return nil
}
func (c *Client) execute(ctx context.Context, args []string) (any, error) {
	if len(args) == 0 {
		return nil, errInput
	}
	switch args[0] {
	case "scenario":
		return c.scenario(ctx, args[1:])
	case "publish":
		return c.publish(ctx, args[1:])
	case "wait":
		return c.wait(ctx, args[1:])
	case "replay":
		return c.replay(ctx, args[1:])
	case "run":
		return nil, errors.New("run choreography is deferred; use individual commands")
	}
	return nil, errInput
}
func (c *Client) scenario(ctx context.Context, args []string) (any, error) {
	if len(args) == 0 {
		return nil, errInput
	}
	command := args[0]
	if command != "create" && command != "recover" && command != "inspect" {
		return nil, errInput
	}
	fs := flag.NewFlagSet("scenario", flag.ContinueOnError)
	id := fs.String("id", "", "")
	code := fs.String("receiver", "", "")
	var mode, dedupe *string
	if command == "create" {
		mode = fs.String("mode", "normal", "")
		dedupe = fs.String("dedupe", "on", "")
	}
	if strictFlags(fs, args[1:]) != nil || !store.ValidScenarioID(*id) {
		return nil, errInput
	}
	codes, err := receivers(*code, command != "recover")
	if err != nil {
		return nil, err
	}
	scenarioID := *id
	if command == "create" {
		if len(*id) > 47 || !validMode(*mode) || (*dedupe != "on" && *dedupe != "off") {
			return nil, errInput
		}
		var suffix [8]byte
		if _, err = rand.Read(suffix[:]); err != nil {
			return nil, errors.New("run identity unavailable")
		}
		scenarioID += "_" + hex.EncodeToString(suffix[:])
		raw, _ := json.Marshal(map[string]any{"scenario_id": scenarioID, "mode": *mode, "dedupe_enabled": *dedupe == "on"})
		for _, receiver := range codes {
			if err = c.request(ctx, "POST", c.endpoints.Controls[receiver]+"/control/scenarios", c.settings.DemoControlToken(), "", raw, nil); err != nil {
				return nil, fmt.Errorf("scenario creation incomplete (run %s): %w", scenarioID, err)
			}
		}
		return struct {
			ScenarioID string   `json:"scenario_id"`
			Receivers  []string `json:"receivers"`
		}{scenarioID, codes}, nil
	}
	if command == "recover" {
		if err = c.request(ctx, "PATCH", c.endpoints.Controls[codes[0]]+"/control/scenarios/"+scenarioID, c.settings.DemoControlToken(), "", []byte(`{"mode":"normal"}`), nil); err != nil {
			return nil, err
		}
		return map[string]string{"mode": "normal", "receiver": codes[0]}, nil
	}
	type summary struct {
		Receiver string `json:"receiver"`
		Mode     string `json:"mode"`
		Dedupe   bool   `json:"dedupe_enabled"`
		Drop     int    `json:"drop_remaining"`
		Requests int64  `json:"request_count"`
		Effects  int64  `json:"effect_count"`
	}
	items := []summary{}
	for _, code := range codes {
		var value summary
		if err = c.request(ctx, "GET", c.endpoints.Controls[code]+"/control/scenarios/"+scenarioID, c.settings.DemoControlToken(), "", nil, &value); err != nil {
			return nil, err
		}
		if !validMode(value.Mode) || value.Drop < 0 || value.Drop > 1 || value.Requests < 0 || value.Effects < 0 {
			return nil, errResponse
		}
		value.Receiver = code
		items = append(items, value)
	}
	return items, nil
}
func (c *Client) publish(ctx context.Context, args []string) (any, error) {
	fs := flag.NewFlagSet("publish", flag.ContinueOnError)
	scenario := fs.String("scenario", "", "")
	order := fs.String("order", "", "")
	dest := fs.String("destinations", "", "")
	quantity := fs.Int("quantity", 1, "")
	key := fs.String("key", "", "")
	file := fs.String("file", "", "")
	if strictFlags(fs, args) != nil {
		return nil, errInput
	}
	var raw []byte
	if *file != "" {
		if *scenario != "" || *order != "" || *dest != "" || *key == "" {
			return nil, errInput
		}
		f, err := os.Open(*file)
		if err != nil {
			return nil, errors.New("request file unavailable")
		}
		raw, err = io.ReadAll(io.LimitReader(f, store.MaxEventRequestBytes+1))
		f.Close()
		if err != nil {
			return nil, errors.New("request file unreadable")
		}
	} else {
		if !store.ValidScenarioID(*scenario) || !utf8.ValidString(*order) || utf8.RuneCountInString(*order) < 1 || utf8.RuneCountInString(*order) > 128 || *quantity < 1 || *quantity > 100000 {
			return nil, errInput
		}
		codes := strings.Split(*dest, ",")
		sort.Strings(codes)
		ids := []string{}
		seen := map[string]bool{}
		for _, code := range codes {
			if seen[code] {
				return nil, errInput
			}
			seen[code] = true
			switch code {
			case "fulfillment":
				ids = append(ids, config.FulfillmentDestination)
			case "notification":
				ids = append(ids, config.NotificationDestination)
			default:
				return nil, errInput
			}
		}
		// Explicit lower-case field names preserve deterministic request bytes.
		raw, _ = json.Marshal(map[string]any{"type": "order.confirmed", "payload": map[string]any{"scenario_id": *scenario, "order_id": *order, "quantity": *quantity}, "destination_ids": ids})
	}
	if _, err := store.ValidateEventRequest(raw); err != nil {
		return nil, errInput
	}
	if *key == "" {
		sum := sha256.Sum256(raw)
		*key = "demo-" + hex.EncodeToString(sum[:])
	}
	if store.ValidateIdempotencyKey(*key) != nil {
		return nil, errInput
	}
	var result struct {
		EventID     string   `json:"event_id"`
		DeliveryIDs []string `json:"delivery_ids"`
	}
	if err := c.request(ctx, "POST", c.endpoints.API+"/v1/events", c.settings.APIToken(), *key, raw, &result); err != nil {
		return nil, err
	}
	if !uuid(result.EventID) || len(result.DeliveryIDs) < 1 || len(result.DeliveryIDs) > 2 {
		return nil, errResponse
	}
	for _, id := range result.DeliveryIDs {
		if !uuid(id) {
			return nil, errResponse
		}
	}
	return result, nil
}
func (c *Client) replay(ctx context.Context, args []string) (any, error) {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	id := fs.String("delivery", "", "")
	key := fs.String("key", "", "")
	if strictFlags(fs, args) != nil || !uuid(*id) || store.ValidateIdempotencyKey(*key) != nil {
		return nil, errInput
	}
	var result struct {
		ReplayID     string `json:"replay_id"`
		DeliveryID   string `json:"delivery_id"`
		Cycle        int    `json:"cycle_no"`
		Deduplicated bool   `json:"deduplicated"`
	}
	if err := c.request(ctx, "POST", c.endpoints.API+"/v1/deliveries/"+*id+"/replay", c.settings.APIToken(), *key, []byte(`{}`), &result); err != nil {
		return nil, err
	}
	if !uuid(result.ReplayID) || result.DeliveryID != *id || result.Cycle < 2 {
		return nil, errResponse
	}
	return result, nil
}
func (c *Client) wait(ctx context.Context, args []string) (any, error) {
	fs := flag.NewFlagSet("wait", flag.ContinueOnError)
	id := fs.String("event", "", "")
	state := fs.String("state", "", "")
	timeout := fs.Duration("timeout", 90*time.Second, "")
	if strictFlags(fs, args) != nil || !uuid(*id) || (*state != "succeeded" && *state != "dead") || *timeout <= 0 || *timeout > 10*time.Minute || c.PollInterval <= 0 {
		return nil, errInput
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	last := "unavailable"
	for {
		var result struct {
			ID         string `json:"id"`
			Deliveries []struct {
				ID     string `json:"id"`
				Code   string `json:"destination_code"`
				Status string `json:"status"`
			} `json:"deliveries"`
		}
		err := c.request(ctx, "GET", c.endpoints.API+"/v1/events/"+*id, c.settings.APIToken(), "", nil, &result)
		if ctx.Err() != nil {
			return nil, fmt.Errorf("wait deadline or cancellation (last: %s)", last)
		}
		if err != nil {
			return nil, err
		}
		if result.ID != *id || len(result.Deliveries) < 1 || len(result.Deliveries) > 2 {
			return nil, errResponse
		}
		all := true
		statuses := []string{}
		for _, d := range result.Deliveries {
			if !uuid(d.ID) || (d.Code != "fulfillment" && d.Code != "notification") {
				return nil, errResponse
			}
			switch d.Status {
			case "pending", "in_flight", "succeeded", "dead":
			default:
				return nil, errResponse
			}
			statuses = append(statuses, d.Code+"="+d.Status)
			all = all && d.Status == *state
		}
		sort.Strings(statuses)
		last = strings.Join(statuses, ",")
		if all {
			return map[string]string{"event_id": *id, "state": *state}, nil
		}
		timer := time.NewTimer(c.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("wait deadline or cancellation (last: %s)", last)
		case <-timer.C:
		}
	}
}
