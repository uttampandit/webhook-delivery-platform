package benchmark

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"time"
)

func writeIngestSamples(path string, samples []requestSample) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	err = writer.Write([]string{
		"sequence", "outcome", "error_class", "http_status", "client_elapsed_ns",
		"request_bytes", "deduplicated", "api_event_id", "database_event_id", "database_request_bytes", "api_delivery_ids",
	})
	for _, sample := range samples {
		if err != nil {
			break
		}
		writer.Write([]string{
			strconv.Itoa(sample.Sequence), sample.Outcome, sample.ErrorClass,
			strconv.Itoa(sample.HTTPStatus), strconv.FormatInt(sample.LatencyNS, 10),
			strconv.Itoa(sample.RequestBytes), strconv.FormatBool(sample.Deduplicated),
			sample.EventID, sample.DatabaseEventID, strconv.Itoa(sample.DatabaseRequestBytes),
			strings.Join(sortedIDs(sample.DeliveryIDs), ";"),
		})
	}
	writer.Flush()
	if err == nil {
		err = writer.Error()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func writeDeliverySamples(path string, deliveries []deliveryRow) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	writer := csv.NewWriter(file)
	err = writer.Write([]string{
		"delivery_id", "event_id", "destination", "status", "persisted_attempts",
		"event_created_at", "first_attempt_started_at", "confirmed_at", "initial_queue_delay_ms", "acknowledgment_latency_ms",
	})
	for _, delivery := range deliveries {
		if err != nil {
			break
		}
		queueDelay, acknowledgment := "", ""
		if delivery.FirstStart != nil {
			queueDelay = strconv.FormatFloat(float64(delivery.FirstStart.Sub(delivery.CreatedAt))/float64(time.Millisecond), 'f', 3, 64)
		}
		if delivery.ConfirmedAt != nil && delivery.Status == "succeeded" {
			acknowledgment = strconv.FormatFloat(float64(delivery.ConfirmedAt.Sub(delivery.CreatedAt))/float64(time.Millisecond), 'f', 3, 64)
		}
		firstStarted, confirmed := "", ""
		if delivery.FirstStart != nil {
			firstStarted = delivery.FirstStart.UTC().Format(time.RFC3339Nano)
		}
		if delivery.ConfirmedAt != nil {
			confirmed = delivery.ConfirmedAt.UTC().Format(time.RFC3339Nano)
		}
		writer.Write([]string{
			delivery.ID, delivery.EventID, delivery.Destination, delivery.Status,
			strconv.Itoa(delivery.Attempts), delivery.CreatedAt.UTC().Format(time.RFC3339Nano),
			firstStarted, confirmed, queueDelay, acknowledgment,
		})
	}
	writer.Flush()
	if err == nil {
		err = writer.Error()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
