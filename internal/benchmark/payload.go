package benchmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/uttampandit/webhook-delivery-platform/internal/config"
	"github.com/uttampandit/webhook-delivery-platform/internal/store"
)

type ingestRequest struct {
	Type           string        `json:"type"`
	Payload        ingestPayload `json:"payload"`
	DestinationIDs []string      `json:"destination_ids"`
}

type ingestPayload struct {
	ScenarioID string `json:"scenario_id"`
	OrderID    string `json:"order_id"`
	Quantity   int    `json:"quantity"`
	Padding    string `json:"padding"`
}

// BuildIngestBody pads the complete serialized request to an exact byte size.
// The API stores this request before it constructs the receiver envelope.
func BuildIngestBody(scenarioID, orderID string, payloadBytes int) ([]byte, error) {
	request := ingestRequest{
		Type:           "order.confirmed",
		Payload:        ingestPayload{ScenarioID: scenarioID, OrderID: orderID, Quantity: 1},
		DestinationIDs: []string{config.FulfillmentDestination, config.NotificationDestination},
	}
	base, err := json.Marshal(request)
	if err != nil {
		return nil, errors.New("benchmark request encoding failed")
	}
	if payloadBytes < len(base) {
		return nil, fmt.Errorf("payload size %d is below the minimum serialized request size %d bytes", payloadBytes, len(base))
	}
	if payloadBytes > store.MaxEventRequestBytes {
		return nil, fmt.Errorf("payload size exceeds the API request limit of %d bytes", store.MaxEventRequestBytes)
	}
	request.Payload.Padding = strings.Repeat("x", payloadBytes-len(base))
	raw, err := json.Marshal(request)
	if err != nil {
		return nil, errors.New("benchmark request encoding failed")
	}
	if len(raw) != payloadBytes {
		return nil, errors.New("benchmark request did not reach the requested serialized size")
	}
	if _, err = store.ValidateEventRequest(raw); err != nil {
		return nil, errors.New("benchmark request failed local validation")
	}
	return raw, nil
}
