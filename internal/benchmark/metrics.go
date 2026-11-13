package benchmark

import "time"

func reconcileReport(report *Report, snap snapshot, samples []requestSample, keys []string, keySequence map[string]int) []requestSample {
	result := ReconciliationReport{
		APIResponseEventIDsMatchDatabase: true,
		ReturnedDeliveryIDsMatchDatabase: true,
		DeliveryCountMatchesAccepted:     true,
		ReceiverConfigurationMatches:     true,
		ReceiverOperationsMatch:          true,
		ReceiverEffectsDeduplicated:      true,
		ExpectedDeliveries:               len(snap.Events) * 2,
		Issues:                           []string{},
	}
	eventsByKey := make(map[string]eventRow, len(snap.Events))
	for _, event := range snap.Events {
		eventsByKey[event.IdempotencyKey] = event
		sequence := keySequence[event.IdempotencyKey]
		if sequence > 0 && sequence <= len(samples) {
			samples[sequence-1].DatabaseEventID = event.ID
			samples[sequence-1].DatabaseRequestBytes = event.RequestBytes
		}
	}
	deliveriesByEvent := map[string][]deliveryRow{}
	knownOperations := map[string]map[string]bool{"fulfillment": {}, "notification": {}}
	deliveriesByDestination := map[string]int{}
	for _, delivery := range snap.Deliveries {
		deliveriesByEvent[delivery.EventID] = append(deliveriesByEvent[delivery.EventID], delivery)
		if knownOperations[delivery.Destination] != nil {
			knownOperations[delivery.Destination][delivery.ID] = true
			deliveriesByDestination[delivery.Destination]++
		}
	}
	for _, sample := range samples {
		if sample.EventID == "" {
			continue
		}
		if sample.Sequence < 1 || sample.Sequence > len(keys) {
			result.APIResponseEventIDsMatchDatabase = false
			result.ReturnedDeliveryIDsMatchDatabase = false
			continue
		}
		event, exists := eventsByKey[keys[sample.Sequence-1]]
		if !exists || event.ID != sample.EventID {
			result.APIResponseEventIDsMatchDatabase = false
			result.ReturnedDeliveryIDsMatchDatabase = false
			continue
		}
		actual := make([]string, 0, 2)
		for _, delivery := range deliveriesByEvent[event.ID] {
			actual = append(actual, delivery.ID)
		}
		if !sameStrings(sortedIDs(actual), sortedIDs(sample.DeliveryIDs)) {
			result.ReturnedDeliveryIDsMatchDatabase = false
		}
	}
	if len(snap.Deliveries) != result.ExpectedDeliveries {
		result.DeliveryCountMatchesAccepted = false
	}
	for _, event := range snap.Events {
		codes := map[string]bool{}
		for _, delivery := range deliveriesByEvent[event.ID] {
			codes[delivery.Destination] = true
		}
		if len(codes) != 2 || !codes["fulfillment"] || !codes["notification"] {
			result.DeliveryCountMatchesAccepted = false
		}
	}

	if len(snap.Receivers) != 2 {
		result.ReceiverConfigurationMatches = false
	}
	for _, code := range []string{"fulfillment", "notification"} {
		receiver, exists := snap.Receivers[code]
		if !exists || receiver.Mode != "normal" || !receiver.Dedupe {
			result.ReceiverConfigurationMatches = false
			result.ReceiverEffectsDeduplicated = false
			continue
		}
		if int64(len(receiver.RequestOps)) != receiver.RequestOperationCount || int64(len(receiver.EffectOps)) != receiver.EffectOperationCount {
			result.ReceiverOperationsMatch = false
			result.ReceiverEffectsDeduplicated = false
		}
		for _, operation := range receiver.RequestOps {
			if !knownOperations[code][operation] {
				result.ReceiverOperationsMatch = false
			}
		}
		for _, operation := range receiver.EffectOps {
			if !knownOperations[code][operation] {
				result.ReceiverOperationsMatch = false
				result.ReceiverEffectsDeduplicated = false
			}
		}
		if receiver.Effects != receiver.EffectOperationCount || receiver.Effects > int64(deliveriesByDestination[code]) {
			result.ReceiverEffectsDeduplicated = false
		}
	}
	if !result.APIResponseEventIDsMatchDatabase {
		result.Issues = append(result.Issues, "api_event_id_mismatch")
	}
	if !result.ReturnedDeliveryIDsMatchDatabase {
		result.Issues = append(result.Issues, "api_delivery_id_mismatch")
	}
	if !result.DeliveryCountMatchesAccepted {
		result.Issues = append(result.Issues, "delivery_count_mismatch")
	}
	if !result.ReceiverConfigurationMatches {
		result.Issues = append(result.Issues, "receiver_configuration_mismatch")
	}
	if !result.ReceiverOperationsMatch {
		result.Issues = append(result.Issues, "receiver_operation_mismatch")
	}
	if !result.ReceiverEffectsDeduplicated {
		result.Issues = append(result.Issues, "receiver_effect_deduplication_mismatch")
	}
	result.Passed = len(result.Issues) == 0
	report.Reconciliation = result
	return samples
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func populateCountsAndPerformance(report *Report, requests []requestSample, snap snapshot, ingestElapsed time.Duration) {
	counts := report.Counts
	counts.AcceptedUniqueEvents = len(snap.Events)
	counts.Deliveries = len(snap.Deliveries)
	counts.RequestFailures = map[string]int{}
	clientAll := []float64{}
	clientSuccess := []float64{}
	inputSizes := []float64{}
	storedSizes := []float64{}
	wireSizes := []float64{}
	for _, request := range requests {
		if request.ErrorClass != "" {
			counts.RequestFailures[request.ErrorClass]++
		}
		if request.Attempted {
			clientAll = append(clientAll, float64(request.LatencyNS)/float64(time.Millisecond))
			inputSizes = append(inputSizes, float64(request.RequestBytes))
		}
		if request.Outcome == "accepted" || request.Outcome == "deduplicated" {
			clientSuccess = append(clientSuccess, float64(request.LatencyNS)/float64(time.Millisecond))
		}
	}
	for _, event := range snap.Events {
		storedSizes = append(storedSizes, float64(event.RequestBytes))
		wireSizes = append(wireSizes, float64(event.WireBytes))
	}
	allQueueDelays := []float64{}
	allAckLatencies := []float64{}
	queueByDestination := map[string][]float64{"fulfillment": {}, "notification": {}}
	ackByDestination := map[string][]float64{"fulfillment": {}, "notification": {}}
	attemptsByDestination := map[string]int{}
	deliveriesByDestination := map[string]int{}
	statusCount := map[string]map[string]int{
		"fulfillment":  {"succeeded": 0, "dead": 0, "pending": 0, "in_flight": 0},
		"notification": {"succeeded": 0, "dead": 0, "pending": 0, "in_flight": 0},
	}
	for _, delivery := range snap.Deliveries {
		counts.PersistedAttempts += delivery.Attempts
		deliveriesByDestination[delivery.Destination]++
		attemptsByDestination[delivery.Destination] += delivery.Attempts
		if statusCount[delivery.Destination] != nil {
			statusCount[delivery.Destination][delivery.Status]++
		}
		if delivery.FirstStart != nil {
			value := float64(delivery.FirstStart.Sub(delivery.CreatedAt)) / float64(time.Millisecond)
			allQueueDelays = append(allQueueDelays, value)
			if queueByDestination[delivery.Destination] != nil {
				queueByDestination[delivery.Destination] = append(queueByDestination[delivery.Destination], value)
			}
		}
		if delivery.Status == "succeeded" && delivery.ConfirmedAt != nil {
			value := float64(delivery.ConfirmedAt.Sub(delivery.CreatedAt)) / float64(time.Millisecond)
			allAckLatencies = append(allAckLatencies, value)
			if ackByDestination[delivery.Destination] != nil {
				ackByDestination[delivery.Destination] = append(ackByDestination[delivery.Destination], value)
			}
		}
		switch delivery.Status {
		case "succeeded":
			counts.SucceededDeliveries++
		case "dead":
			counts.DeadDeliveries++
		case "pending":
			counts.PendingDeliveries++
		case "in_flight":
			counts.InFlightDeliveries++
		}
	}
	for _, receiver := range snap.Receivers {
		counts.ReceiverRequests += receiver.Requests
		counts.ReceiverEffects += receiver.Effects
	}
	counts.RequestFailures = normalizeFailureCounts(counts.RequestFailures)
	report.Counts = counts
	acceptedRate := 0.0
	if ingestElapsed > 0 {
		acceptedRate = float64(len(snap.Events)) / ingestElapsed.Seconds()
	}
	report.Performance = PerformanceReport{
		IngestPhaseMilliseconds:      milliseconds(ingestElapsed),
		DrainPhaseMilliseconds:       report.Performance.DrainPhaseMilliseconds,
		AcceptedEventsPerSecond:      round3(acceptedRate),
		RetryAmplification:           ratio(counts.PersistedAttempts, counts.Deliveries),
		ReceiverRequestAmplification: ratio64(counts.ReceiverRequests, counts.Deliveries),
		APIRequestLatencyAllMS:       summarize(clientAll),
		APIRequestLatencySuccessMS:   summarize(clientSuccess),
		InitialQueueDelayMS:          summarize(allQueueDelays),
		AcknowledgmentLatencyMS:      summarize(allAckLatencies),
		InputRequestBytes:            summarize(inputSizes),
		StoredRequestBytes:           summarize(storedSizes),
		WireBodyBytes:                summarize(wireSizes),
		Destinations:                 map[string]DestinationReport{},
		Definitions:                  metricDefinitions(),
	}
	for _, code := range []string{"fulfillment", "notification"} {
		deliveryCount := deliveriesByDestination[code]
		receiver := snap.Receivers[code]
		perDestination := DestinationReport{
			Deliveries: deliveryCount,
			Succeeded:  statusCount[code]["succeeded"], Dead: statusCount[code]["dead"],
			Unfinished:        statusCount[code]["pending"] + statusCount[code]["in_flight"],
			PersistedAttempts: attemptsByDestination[code],
			ReceiverRequests:  receiver.Requests, ReceiverEffects: receiver.Effects,
			RetryAmplification:      ratio(attemptsByDestination[code], deliveryCount),
			RequestAmplification:    ratio64(receiver.Requests, deliveryCount),
			InitialQueueDelayMS:     summarize(queueByDestination[code]),
			AcknowledgmentLatencyMS: summarize(ackByDestination[code]),
		}
		report.Performance.Destinations[code] = perDestination
	}
}

func normalizeFailureCounts(values map[string]int) map[string]int {
	if len(values) == 0 {
		return map[string]int{}
	}
	result := make(map[string]int, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func ratio(numerator, denominator int) *float64 {
	if denominator == 0 {
		return nil
	}
	value := round3(float64(numerator) / float64(denominator))
	return &value
}

func ratio64(numerator int64, denominator int) *float64 {
	if denominator == 0 {
		return nil
	}
	value := round3(float64(numerator) / float64(denominator))
	return &value
}
