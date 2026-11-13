package democtl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

const (
	demoRunTimeout      = 15 * time.Minute
	demoScenarioTimeout = 10 * time.Minute
)

type runReportedError struct {
	report runReport
	err    error
}

func (e *runReportedError) Error() string { return e.err.Error() }
func (e *runReportedError) Unwrap() error { return e.err }

type runReport struct {
	SchemaVersion int               `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Status        string            `json:"status"`
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at"`
	Failure       *runFailureDetail `json:"failure,omitempty"`
	Scenarios     []scenarioReport  `json:"scenarios"`
}

type runFailureDetail struct {
	Step   string `json:"step"`
	Reason string `json:"reason"`
}

type scenarioReport struct {
	Name         string                      `json:"name"`
	Status       string                      `json:"status"`
	ScenarioID   string                      `json:"scenario_id,omitempty"`
	EventID      string                      `json:"event_id,omitempty"`
	ReplayID     string                      `json:"replay_id,omitempty"`
	DeliveryIDs  map[string]string           `json:"delivery_ids,omitempty"`
	OperationIDs map[string]string           `json:"operation_ids,omitempty"`
	Observations []runObservation            `json:"observations"`
	Receivers    map[string]receiverEvidence `json:"receivers,omitempty"`
	Checks       []string                    `json:"checks"`
	Notes        []string                    `json:"notes,omitempty"`
}

type runObservation struct {
	Stage      string                      `json:"stage"`
	ObservedAt time.Time                   `json:"observed_at"`
	Deliveries map[string]deliveryEvidence `json:"deliveries"`
}

type deliveryEvidence struct {
	ID                     string            `json:"id"`
	OperationID            string            `json:"operation_id"`
	Status                 string            `json:"status"`
	CycleNo                int               `json:"cycle_no"`
	CycleAttemptCount      int               `json:"cycle_attempt_count"`
	LifetimeAttemptCount   int               `json:"lifetime_attempt_count"`
	HasUnconfirmedAttempts bool              `json:"has_unconfirmed_attempts"`
	Attempts               []attemptEvidence `json:"attempts"`
}

type attemptEvidence struct {
	AttemptNo  int    `json:"attempt_no"`
	CycleNo    int    `json:"cycle_no"`
	Outcome    string `json:"outcome"`
	HTTPStatus *int   `json:"http_status,omitempty"`
}

type receiverEvidence struct {
	Mode            string              `json:"mode"`
	DedupeEnabled   bool                `json:"dedupe_enabled"`
	DropRemaining   int                 `json:"drop_remaining"`
	RequestCount    int64               `json:"request_count"`
	EffectCount     int64               `json:"effect_count"`
	OperationCounts []operationEvidence `json:"operation_counts"`
}

type operationEvidence struct {
	OperationID  string `json:"operation_id"`
	RequestCount int64  `json:"request_count"`
	EffectCount  int64  `json:"effect_count"`
}

type scenarioCreated struct {
	ScenarioID string   `json:"scenario_id"`
	Receivers  []string `json:"receivers"`
}

type receiverSummary struct {
	Receiver        string `json:"receiver"`
	Mode            string `json:"mode"`
	DedupeEnabled   bool   `json:"dedupe_enabled"`
	DropRemaining   int    `json:"drop_remaining"`
	RequestCount    int64  `json:"request_count"`
	EffectCount     int64  `json:"effect_count"`
	OperationCounts []struct {
		OperationID  string `json:"operation_id"`
		RequestCount int64  `json:"request_count"`
		EffectCount  int64  `json:"effect_count"`
	} `json:"operation_counts"`
}

type publishResponse struct {
	EventID     string   `json:"event_id"`
	DeliveryIDs []string `json:"delivery_ids"`
}

type eventDetail struct {
	ID         string          `json:"id"`
	Deliveries []eventDelivery `json:"deliveries"`
}

type eventDelivery struct {
	ID                     string         `json:"id"`
	OperationID            string         `json:"operation_id"`
	DestinationCode        string         `json:"destination_code"`
	Status                 string         `json:"status"`
	CycleNo                int            `json:"cycle_no"`
	CycleAttemptCount      int            `json:"cycle_attempt_count"`
	LifetimeAttemptCount   int            `json:"lifetime_attempt_count"`
	MaxAttempts            int            `json:"max_attempts"`
	HasUnconfirmedAttempts bool           `json:"has_unconfirmed_attempts"`
	Attempts               []eventAttempt `json:"attempts"`
	Replays                []eventReplay  `json:"replays"`
}

type eventAttempt struct {
	AttemptNo  int    `json:"attempt_no"`
	CycleNo    int    `json:"cycle_no"`
	Outcome    string `json:"outcome"`
	HTTPStatus *int   `json:"http_status"`
}

type eventReplay struct {
	ID        string `json:"id"`
	FromCycle int    `json:"from_cycle"`
	ToCycle   int    `json:"to_cycle"`
}

type replayResponse struct {
	ReplayID     string `json:"replay_id"`
	DeliveryID   string `json:"delivery_id"`
	Cycle        int    `json:"cycle_no"`
	Deduplicated bool   `json:"deduplicated"`
}

func (c *Client) run(parent context.Context) (any, error) {
	runID, err := newRunID()
	if err != nil {
		return nil, err
	}
	started := time.Now().UTC()
	report := runReport{
		SchemaVersion: 1,
		RunID:         runID,
		Status:        "running",
		StartedAt:     started,
		Scenarios:     make([]scenarioReport, 0, 5),
	}
	ctx, cancel := context.WithTimeout(parent, demoRunTimeout)
	defer cancel()

	steps := []struct {
		name string
		run  func(context.Context, *scenarioReport, string) error
	}{
		{name: "healthy_fanout", run: c.runHealthyFanout},
		{name: "outage_isolation_recovery", run: c.runOutageRecovery},
		{name: "lost_response_dedupe_on", run: c.runLostResponseDedupe},
		{name: "lost_response_dedupe_off", run: c.runLostResponseUnsafe},
		{name: "dead_delivery_replay", run: c.runDeadReplay},
	}
	for _, step := range steps {
		report.Scenarios = append(report.Scenarios, scenarioReport{
			Name:         step.name,
			Status:       "running",
			DeliveryIDs:  map[string]string{},
			OperationIDs: map[string]string{},
			Observations: []runObservation{},
			Receivers:    map[string]receiverEvidence{},
			Checks:       []string{},
		})
		current := &report.Scenarios[len(report.Scenarios)-1]
		if err := step.run(ctx, current, runID); err != nil {
			report.Status = "failed"
			report.FinishedAt = time.Now().UTC()
			current.Status = "failed"
			report.Failure = &runFailureDetail{Step: step.name, Reason: safeFailureReason(err)}
			return nil, &runReportedError{report: report, err: fmt.Errorf("demo failed at %s: %w", step.name, err)}
		}
		current.Status = "passed"
	}
	if err := ctx.Err(); err != nil {
		report.Status = "failed"
		report.FinishedAt = time.Now().UTC()
		report.Failure = &runFailureDetail{Step: "run_deadline", Reason: "overall run deadline or cancellation"}
		return nil, &runReportedError{report: report, err: errors.New("demo run deadline or cancellation")}
	}
	report.Status = "passed"
	report.FinishedAt = time.Now().UTC()
	return report, nil
}

func newRunID() (string, error) {
	var suffix [6]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", errors.New("run identity unavailable")
	}
	return "demo_" + time.Now().UTC().Format("20060102T150405Z") + "_" + hex.EncodeToString(suffix[:]), nil
}

func safeFailureReason(err error) string {
	if err == nil {
		return "unknown failure"
	}
	return err.Error()
}

func (c *Client) runHealthyFanout(ctx context.Context, result *scenarioReport, runID string) error {
	scenarioID, err := c.createScenario(ctx, runID+"_healthy", "both", "normal", "on")
	if err != nil {
		return err
	}
	result.ScenarioID = scenarioID
	eventID, _, err := c.publishScenario(ctx, scenarioID, runID+"_healthy", []string{"fulfillment", "notification"})
	if err != nil {
		return err
	}
	result.EventID = eventID
	if err = c.waitEventState(ctx, eventID, "succeeded"); err != nil {
		return err
	}
	detail, err := c.getEvent(ctx, eventID)
	if err != nil {
		return err
	}
	result.observe("both_destinations_succeeded", detail)
	for _, code := range []string{"fulfillment", "notification"} {
		delivery, ok := detail.delivery(code)
		if !ok || delivery.Status != "succeeded" {
			return fmt.Errorf("healthy fan-out did not succeed at %s", code)
		}
		result.DeliveryIDs[code] = delivery.ID
		result.OperationIDs[code] = delivery.OperationID
		if err = c.captureReceiver(ctx, result, scenarioID, code); err != nil {
			return err
		}
		summary := result.Receivers[code]
		if summary.RequestCount != 1 || summary.EffectCount != 1 || !summary.DedupeEnabled || summary.Mode != "normal" {
			return fmt.Errorf("healthy receiver counts failed for %s", code)
		}
	}
	result.Checks = append(result.Checks, "both deliveries succeeded; each receiver recorded one request and one effect")
	return nil
}

func (c *Client) runOutageRecovery(ctx context.Context, result *scenarioReport, runID string) error {
	scenarioID, err := c.createScenario(ctx, runID+"_outage", "both", "http_503", "on")
	if err != nil {
		return err
	}
	result.ScenarioID = scenarioID
	if err = c.recoverScenario(ctx, scenarioID, "notification"); err != nil {
		return err
	}
	eventID, _, err := c.publishScenario(ctx, scenarioID, runID+"_outage", []string{"fulfillment", "notification"})
	if err != nil {
		return err
	}
	result.EventID = eventID
	initial, err := c.waitForEvent(ctx, eventID, func(detail eventDetail) bool {
		fulfillment, hasFulfillment := detail.delivery("fulfillment")
		notification, hasNotification := detail.delivery("notification")
		return hasFulfillment && hasNotification && notification.Status == "succeeded" &&
			hasHTTPFailure(fulfillment, 503) && (fulfillment.Status == "pending" || fulfillment.Status == "in_flight" || fulfillment.Status == "dead")
	}, "notification success with a recorded fulfillment 503")
	if err != nil {
		return err
	}
	result.observe("notification_succeeded_while_fulfillment_was_failing", initial)
	result.Checks = append(result.Checks, "notification succeeded while fulfillment recorded HTTP 503")
	if err = c.recoverScenario(ctx, scenarioID, "fulfillment"); err != nil {
		return err
	}
	resolved, err := c.waitForEvent(ctx, eventID, func(detail eventDetail) bool {
		fulfillment, ok := detail.delivery("fulfillment")
		return ok && (fulfillment.Status == "dead" || fulfillment.Status == "succeeded")
	}, "fulfillment recovery or exhaustion")
	if err != nil {
		return err
	}
	fulfillment, _ := resolved.delivery("fulfillment")
	if fulfillment.Status == "dead" {
		result.observe("fulfillment_exhausted_before_recovery_took_effect", resolved)
		replay, replayErr := c.replayDelivery(ctx, fulfillment.ID, runID+"_outage_replay")
		if replayErr != nil {
			return replayErr
		}
		result.ReplayID = replay.ReplayID
		result.Notes = append(result.Notes, "fulfillment exhausted its first cycle before recovery took effect; it was explicitly replayed")
	}
	if err = c.waitEventState(ctx, eventID, "succeeded"); err != nil {
		return err
	}
	final, err := c.getEvent(ctx, eventID)
	if err != nil {
		return err
	}
	result.observe("both_destinations_succeeded_after_recovery", final)
	for _, code := range []string{"fulfillment", "notification"} {
		delivery, ok := final.delivery(code)
		if !ok || delivery.Status != "succeeded" {
			return fmt.Errorf("outage recovery did not succeed at %s", code)
		}
		result.DeliveryIDs[code] = delivery.ID
		result.OperationIDs[code] = delivery.OperationID
		if err = c.captureReceiver(ctx, result, scenarioID, code); err != nil {
			return err
		}
		summary := result.Receivers[code]
		if summary.EffectCount != 1 || summary.RequestCount < 1 {
			return fmt.Errorf("outage recovery receiver counts failed for %s", code)
		}
		if code == "notification" && summary.RequestCount != 1 {
			return errors.New("notification was redelivered during fulfillment outage")
		}
	}
	if result.Receivers["fulfillment"].RequestCount < 2 {
		return errors.New("fulfillment did not record a failed request and a recovery request")
	}
	result.Checks = append(result.Checks, "fulfillment recovered or replayed; notification effect stayed at one")
	return nil
}

func (c *Client) runLostResponseDedupe(ctx context.Context, result *scenarioReport, runID string) error {
	return c.runLostResponse(ctx, result, runID, true)
}

func (c *Client) runLostResponseUnsafe(ctx context.Context, result *scenarioReport, runID string) error {
	result.Notes = append(result.Notes, "deliberately unsafe receiver behavior: deduplication is disabled and the lost response repeats the business effect")
	return c.runLostResponse(ctx, result, runID, false)
}

func (c *Client) runLostResponse(ctx context.Context, result *scenarioReport, runID string, dedupe bool) error {
	base := "_loss_unsafe"
	name := runID + base
	dedupeMode := "off"
	wantEffects := int64(2)
	if dedupe {
		name = runID + "_loss_dedupe"
		dedupeMode = "on"
		wantEffects = 1
	}
	scenarioID, err := c.createScenario(ctx, name, "fulfillment", "commit_drop_once", dedupeMode)
	if err != nil {
		return err
	}
	result.ScenarioID = scenarioID
	eventID, _, err := c.publishScenario(ctx, scenarioID, name, []string{"fulfillment"})
	if err != nil {
		return err
	}
	result.EventID = eventID
	if err = c.waitEventState(ctx, eventID, "succeeded"); err != nil {
		return err
	}
	detail, err := c.getEvent(ctx, eventID)
	if err != nil {
		return err
	}
	delivery, ok := detail.delivery("fulfillment")
	if !ok || delivery.Status != "succeeded" || len(delivery.Attempts) < 2 || !delivery.HasUnconfirmedAttempts || delivery.Attempts[0].Outcome != "unconfirmed" {
		return errors.New("lost-response retry did not preserve an earlier unconfirmed attempt")
	}
	result.DeliveryIDs["fulfillment"] = delivery.ID
	result.OperationIDs["fulfillment"] = delivery.OperationID
	result.observe("delivery_succeeded_after_lost_response", detail)
	if err = c.captureReceiver(ctx, result, scenarioID, "fulfillment"); err != nil {
		return err
	}
	receiver := result.Receivers["fulfillment"]
	if receiver.RequestCount < 2 || receiver.EffectCount != wantEffects || receiver.DedupeEnabled != dedupe || receiver.Mode != "commit_drop_once" {
		return errors.New("lost-response receiver counts did not match the selected dedupe mode")
	}
	if len(receiver.OperationCounts) != 1 || receiver.OperationCounts[0].OperationID != delivery.OperationID || receiver.OperationCounts[0].RequestCount < 2 || receiver.OperationCounts[0].EffectCount != wantEffects {
		return errors.New("lost-response operation counts did not match the delivery")
	}
	if dedupe {
		result.Checks = append(result.Checks, "at least two receiver requests, one committed effect, and an earlier unconfirmed attempt")
	} else {
		result.Checks = append(result.Checks, "at least two receiver requests and two effects with receiver deduplication disabled")
	}
	return nil
}

func (c *Client) runDeadReplay(ctx context.Context, result *scenarioReport, runID string) error {
	scenarioID, err := c.createScenario(ctx, runID+"_replay", "fulfillment", "http_503", "on")
	if err != nil {
		return err
	}
	result.ScenarioID = scenarioID
	eventID, _, err := c.publishScenario(ctx, scenarioID, runID+"_replay", []string{"fulfillment"})
	if err != nil {
		return err
	}
	result.EventID = eventID
	if err = c.waitEventState(ctx, eventID, "dead"); err != nil {
		return err
	}
	dead, err := c.getEvent(ctx, eventID)
	if err != nil {
		return err
	}
	first, ok := dead.delivery("fulfillment")
	if !ok || first.Status != "dead" || first.CycleNo != 1 || first.CycleAttemptCount != 5 || first.LifetimeAttemptCount != 5 || len(first.Attempts) != 5 {
		return errors.New("fulfillment did not exhaust five claims in cycle one")
	}
	for _, attempt := range first.Attempts {
		if attempt.Outcome != "http_failure" || attempt.HTTPStatus == nil || *attempt.HTTPStatus != 503 {
			return errors.New("dead delivery history did not record five HTTP 503 responses")
		}
	}
	result.DeliveryIDs["fulfillment"] = first.ID
	result.OperationIDs["fulfillment"] = first.OperationID
	result.observe("dead_after_five_claims", dead)
	if err = c.recoverScenario(ctx, scenarioID, "fulfillment"); err != nil {
		return err
	}
	replay, err := c.replayDelivery(ctx, first.ID, runID+"_replay_request")
	if err != nil {
		return err
	}
	result.ReplayID = replay.ReplayID
	if replay.Cycle != 2 || replay.Deduplicated {
		return errors.New("replay did not start a new request in cycle two")
	}
	if err = c.waitEventState(ctx, eventID, "succeeded"); err != nil {
		return err
	}
	final, err := c.getEvent(ctx, eventID)
	if err != nil {
		return err
	}
	last, ok := final.delivery("fulfillment")
	if !ok || last.Status != "succeeded" || last.OperationID != first.OperationID || last.CycleNo != 2 || last.CycleAttemptCount != 1 || last.LifetimeAttemptCount != first.LifetimeAttemptCount+1 || len(last.Attempts) != 6 || last.Attempts[5].Outcome != "acknowledged" || !hasReplayToCycle(last, 2) {
		return errors.New("replay changed the operation identity or failed to continue lifetime attempts")
	}
	result.observe("succeeded_after_cycle_two_replay", final)
	if err = c.captureReceiver(ctx, result, scenarioID, "fulfillment"); err != nil {
		return err
	}
	receiver := result.Receivers["fulfillment"]
	if receiver.RequestCount != 6 || receiver.EffectCount != 1 || !receiver.DedupeEnabled {
		return errors.New("dead/replay receiver counts did not show five rejections and one effect")
	}
	result.Checks = append(result.Checks, "cycle two retained the operation ID and lifetime attempt count; one effect committed after five HTTP 503 responses")
	return nil
}

func (c *Client) createScenario(ctx context.Context, base, receiver, mode, dedupe string) (string, error) {
	value, err := c.scenario(ctx, []string{"create", "--id", base, "--receiver", receiver, "--mode", mode, "--dedupe", dedupe})
	if err != nil {
		return "", err
	}
	var created scenarioCreated
	if err = decodeValue(value, &created); err != nil || !store.ValidScenarioID(created.ScenarioID) || len(created.Receivers) == 0 {
		return "", errResponse
	}
	return created.ScenarioID, nil
}

func (c *Client) recoverScenario(ctx context.Context, scenarioID, receiver string) error {
	_, err := c.scenario(ctx, []string{"recover", "--id", scenarioID, "--receiver", receiver})
	return err
}

func (c *Client) inspectScenario(ctx context.Context, scenarioID, receiver string) ([]receiverSummary, error) {
	value, err := c.scenario(ctx, []string{"inspect", "--id", scenarioID, "--receiver", receiver})
	if err != nil {
		return nil, err
	}
	var summaries []receiverSummary
	if err = decodeValue(value, &summaries); err != nil {
		return nil, err
	}
	return summaries, nil
}

func (c *Client) publishScenario(ctx context.Context, scenarioID, order string, destinations []string) (string, []string, error) {
	sort.Strings(destinations)
	value, err := c.publish(ctx, []string{"--scenario", scenarioID, "--order", order, "--destinations", strings.Join(destinations, ",")})
	if err != nil {
		return "", nil, err
	}
	var result publishResponse
	if err = decodeValue(value, &result); err != nil || !uuid(result.EventID) || len(result.DeliveryIDs) != len(destinations) {
		return "", nil, errResponse
	}
	return result.EventID, result.DeliveryIDs, nil
}

func (c *Client) replayDelivery(ctx context.Context, deliveryID, key string) (replayResponse, error) {
	value, err := c.replay(ctx, []string{"--delivery", deliveryID, "--key", key})
	if err != nil {
		return replayResponse{}, err
	}
	var result replayResponse
	if err = decodeValue(value, &result); err != nil || !uuid(result.ReplayID) || result.DeliveryID != deliveryID || result.Cycle < 2 {
		return replayResponse{}, errResponse
	}
	return result, nil
}

func decodeValue(value any, target any) error {
	encoded, err := json.Marshal(value)
	if err != nil || store.ValidateDemoJSON(encoded) != nil || json.Unmarshal(encoded, target) != nil {
		return errResponse
	}
	return nil
}

func (c *Client) waitEventState(ctx context.Context, eventID, state string) error {
	waitCtx, cancel := context.WithTimeout(ctx, demoScenarioTimeout)
	defer cancel()
	_, err := c.wait(waitCtx, []string{"--event", eventID, "--state", state, "--timeout", demoScenarioTimeout.String()})
	return err
}

func (c *Client) waitForEvent(ctx context.Context, eventID string, ready func(eventDetail) bool, description string) (eventDetail, error) {
	if c.PollInterval <= 0 || !uuid(eventID) || ready == nil {
		return eventDetail{}, errInput
	}
	waitCtx, cancel := context.WithTimeout(ctx, demoScenarioTimeout)
	defer cancel()
	last := "not observed"
	for {
		detail, err := c.getEvent(waitCtx, eventID)
		if err != nil {
			if waitCtx.Err() != nil {
				return eventDetail{}, fmt.Errorf("waiting for %s timed out (last: %s)", description, last)
			}
			return eventDetail{}, err
		}
		last = summarizeEvent(detail)
		if ready(detail) {
			return detail, nil
		}
		timer := time.NewTimer(c.PollInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return eventDetail{}, fmt.Errorf("waiting for %s timed out (last: %s)", description, last)
		case <-timer.C:
		}
	}
}

func (c *Client) getEvent(ctx context.Context, eventID string) (eventDetail, error) {
	if !uuid(eventID) {
		return eventDetail{}, errInput
	}
	var detail eventDetail
	if err := c.request(ctx, "GET", c.endpoints.API+"/v1/events/"+eventID, c.settings.APIToken(), "", nil, &detail); err != nil {
		return eventDetail{}, err
	}
	if !validEventDetail(detail, eventID) {
		return eventDetail{}, errResponse
	}
	return detail, nil
}

func validEventDetail(detail eventDetail, eventID string) bool {
	if detail.ID != eventID || len(detail.Deliveries) < 1 || len(detail.Deliveries) > 2 {
		return false
	}
	seen := map[string]bool{}
	for _, delivery := range detail.Deliveries {
		if !uuid(delivery.ID) || delivery.ID != delivery.OperationID || seen[delivery.DestinationCode] || delivery.CycleNo < 1 || delivery.CycleAttemptCount < 0 || delivery.LifetimeAttemptCount < delivery.CycleAttemptCount || delivery.MaxAttempts != 5 || len(delivery.Attempts) > delivery.LifetimeAttemptCount {
			return false
		}
		seen[delivery.DestinationCode] = true
		if delivery.DestinationCode != "fulfillment" && delivery.DestinationCode != "notification" {
			return false
		}
		switch delivery.Status {
		case "pending", "in_flight", "succeeded", "dead":
		default:
			return false
		}
		for _, attempt := range delivery.Attempts {
			if attempt.AttemptNo < 1 || attempt.CycleNo < 1 {
				return false
			}
			switch attempt.Outcome {
			case "running", "acknowledged", "http_failure", "unconfirmed", "local_failure":
			default:
				return false
			}
		}
	}
	return true
}

func (d eventDetail) delivery(code string) (eventDelivery, bool) {
	for _, item := range d.Deliveries {
		if item.DestinationCode == code {
			return item, true
		}
	}
	return eventDelivery{}, false
}

func (r *scenarioReport) observe(stage string, detail eventDetail) {
	observation := runObservation{Stage: stage, ObservedAt: time.Now().UTC(), Deliveries: make(map[string]deliveryEvidence, len(detail.Deliveries))}
	for _, item := range detail.Deliveries {
		attempts := make([]attemptEvidence, 0, len(item.Attempts))
		for _, attempt := range item.Attempts {
			attempts = append(attempts, attemptEvidence{AttemptNo: attempt.AttemptNo, CycleNo: attempt.CycleNo, Outcome: attempt.Outcome, HTTPStatus: attempt.HTTPStatus})
		}
		observation.Deliveries[item.DestinationCode] = deliveryEvidence{
			ID:                     item.ID,
			OperationID:            item.OperationID,
			Status:                 item.Status,
			CycleNo:                item.CycleNo,
			CycleAttemptCount:      item.CycleAttemptCount,
			LifetimeAttemptCount:   item.LifetimeAttemptCount,
			HasUnconfirmedAttempts: item.HasUnconfirmedAttempts,
			Attempts:               attempts,
		}
		r.DeliveryIDs[item.DestinationCode] = item.ID
		r.OperationIDs[item.DestinationCode] = item.OperationID
	}
	r.Observations = append(r.Observations, observation)
}

func (c *Client) captureReceiver(ctx context.Context, result *scenarioReport, scenarioID, code string) error {
	summaries, err := c.inspectScenario(ctx, scenarioID, code)
	if err != nil {
		return err
	}
	if len(summaries) != 1 || summaries[0].Receiver != code || !validMode(summaries[0].Mode) || summaries[0].DropRemaining < 0 || summaries[0].DropRemaining > 1 || summaries[0].RequestCount < 0 || summaries[0].EffectCount < 0 {
		return errResponse
	}
	item := summaries[0]
	operations := make([]operationEvidence, 0, len(item.OperationCounts))
	for _, operation := range item.OperationCounts {
		if !uuid(operation.OperationID) || operation.RequestCount < 0 || operation.EffectCount < 0 {
			return errResponse
		}
		operations = append(operations, operationEvidence{OperationID: operation.OperationID, RequestCount: operation.RequestCount, EffectCount: operation.EffectCount})
	}
	result.Receivers[code] = receiverEvidence{
		Mode:            item.Mode,
		DedupeEnabled:   item.DedupeEnabled,
		DropRemaining:   item.DropRemaining,
		RequestCount:    item.RequestCount,
		EffectCount:     item.EffectCount,
		OperationCounts: operations,
	}
	return nil
}

func summarizeEvent(detail eventDetail) string {
	parts := make([]string, 0, len(detail.Deliveries))
	for _, item := range detail.Deliveries {
		parts = append(parts, fmt.Sprintf("%s=%s(cycle=%d,attempts=%d)", item.DestinationCode, item.Status, item.CycleNo, item.LifetimeAttemptCount))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func hasHTTPFailure(delivery eventDelivery, status int) bool {
	for _, attempt := range delivery.Attempts {
		if attempt.Outcome == "http_failure" && attempt.HTTPStatus != nil && *attempt.HTTPStatus == status {
			return true
		}
	}
	return false
}

func hasReplayToCycle(delivery eventDelivery, cycle int) bool {
	for _, replay := range delivery.Replays {
		if replay.ToCycle == cycle {
			return true
		}
	}
	return false
}
