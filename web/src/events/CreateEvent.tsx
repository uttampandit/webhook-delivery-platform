import { useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { ApiError, apiClient } from "../api/apiClient";
import type { DestinationDto, EventCreateResponseDto } from "../api/dto";
import { createRequestIdentity } from "./requestIdentity";

type EventBody = {
  type: "order.confirmed";
  payload: {
    scenario_id: string;
    order_id: string;
    quantity: number;
  };
  destination_ids: string[];
};

type Submission =
  | { kind: "sending"; identity: ReturnType<typeof createRequestIdentity> }
  | { kind: "uncertain"; identity: ReturnType<typeof createRequestIdentity> }
  | { kind: "accepted"; response: EventCreateResponseDto }
  | { kind: "rejected"; message: string };

export function CreateEvent({
  destinations,
  onOpenEvent,
}: {
  destinations: DestinationDto[];
  onOpenEvent: (eventId: string) => void;
}) {
  const [scenarioId, setScenarioId] = useState("");
  const [orderId, setOrderId] = useState("");
  const [quantity, setQuantity] = useState("1");
  const [destinationIds, setDestinationIds] = useState<string[]>([]);
  const [submission, setSubmission] = useState<Submission | null>(null);
  const [validationMessage, setValidationMessage] = useState("");
  const controllerRef = useRef<AbortController | null>(null);
  const sendingRef = useRef(false);

  useEffect(
    () => () => {
      const controller = controllerRef.current;
      controllerRef.current = null;
      controller?.abort();
    },
    [],
  );

  function edit<T>(setter: (value: T) => void, value: T): void {
    if (submission?.kind === "sending") return;
    setter(value);
    setValidationMessage("");
    setSubmission(null);
  }

  function toggleDestination(id: string): void {
    if (submission?.kind === "sending") return;
    setDestinationIds((current) =>
      current.includes(id) ? current.filter((selected) => selected !== id) : [...current, id],
    );
    setValidationMessage("");
    setSubmission(null);
  }

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (sendingRef.current || submission?.kind === "accepted" || submission?.kind === "rejected") return;

    const normalizedScenario = scenarioId.trim();
    const normalizedOrder = orderId.trim();
    const parsedQuantity = Number(quantity);
    if (!normalizedScenario || !normalizedOrder) {
      setValidationMessage("Enter both an existing scenario ID and an order ID.");
      return;
    }
    if (!Number.isSafeInteger(parsedQuantity) || parsedQuantity < 1) {
      setValidationMessage("Quantity must be a positive whole number.");
      return;
    }
    if (destinationIds.length < 1 || destinationIds.length > 2) {
      setValidationMessage("Choose one or both configured destinations.");
      return;
    }

    const body: EventBody = {
      type: "order.confirmed",
      payload: {
        scenario_id: normalizedScenario,
        order_id: normalizedOrder,
        quantity: parsedQuantity,
      },
      destination_ids: destinations
        .filter((destination) => destinationIds.includes(destination.id))
        .map((destination) => destination.id),
    };
    const identity = submission?.kind === "uncertain"
      ? submission.identity
      : createRequestIdentity(JSON.stringify(body));
    const controller = new AbortController();
    controllerRef.current = controller;
    sendingRef.current = true;
    setValidationMessage("");
    setSubmission({ kind: "sending", identity });

    try {
      const response = await apiClient.createEvent(identity.body, identity.key, controller.signal);
      if (controller.signal.aborted) return;
      setSubmission({ kind: "accepted", response });
    } catch (error) {
      if (controller.signal.aborted) return;
      if (isUncertain(error)) {
        setSubmission({ kind: "uncertain", identity });
      } else {
        setSubmission({ kind: "rejected", message: rejectedMessage(error) });
      }
    } finally {
      sendingRef.current = false;
      if (controllerRef.current === controller) controllerRef.current = null;
    }
  }

  function createAnother(): void {
    if (submission?.kind !== "accepted") return;
    setScenarioId("");
    setOrderId("");
    setQuantity("1");
    setDestinationIds([]);
    setValidationMessage("");
    setSubmission(null);
  }

  const isSending = submission?.kind === "sending";
  const uncertain = submission?.kind === "uncertain";
  const rejected = submission?.kind === "rejected";
  const rejectedMessageValue = submission?.kind === "rejected" ? submission.message : "";
  const accepted = submission?.kind === "accepted" ? submission.response : null;

  return (
    <section aria-labelledby="create-event-heading" className="action-card create-event-card">
      <h3 id="create-event-heading">Create synthetic event</h3>
      <p className="action-description">
        Send an <code>order.confirmed</code> event to one or both fixed destinations.
        An accepted event is queued for delivery; acceptance does not mean a receiver acknowledged it.
      </p>
      <p className="scenario-help">
        Use an existing scenario, or create one with <code>democtl scenario create --id example --receiver both --mode normal --dedupe on</code>.
      </p>

      {accepted ? (
        <div className="action-result accepted-result" role="status">
          <strong>Accepted for processing.</strong>
          <span>Delivery is not yet confirmed.</span>
          <span>Event <code>{accepted.event_id}</code> · {accepted.delivery_ids.length} delivery record(s)</span>
          {accepted.deduplicated ? <span>This exact request was already accepted.</span> : null}
          <div className="action-result-buttons">
            <button className="primary-button" onClick={() => onOpenEvent(accepted.event_id)} type="button">
              View event details
            </button>
            <button className="secondary-button" onClick={createAnother} type="button">
              Create another event
            </button>
          </div>
        </div>
      ) : (
        <form className="create-event-form" onSubmit={(event) => void submit(event)}>
          <label htmlFor="scenario-id">Existing scenario ID</label>
          <input
            autoComplete="off"
            disabled={isSending || destinations.length === 0}
            id="scenario-id"
            maxLength={128}
            onChange={(event) => edit(setScenarioId, event.currentTarget.value)}
            required
            value={scenarioId}
          />
          <label htmlFor="order-id">Order ID</label>
          <input
            autoComplete="off"
            disabled={isSending || destinations.length === 0}
            id="order-id"
            maxLength={128}
            onChange={(event) => edit(setOrderId, event.currentTarget.value)}
            required
            value={orderId}
          />
          <label htmlFor="event-quantity">Quantity</label>
          <input
            disabled={isSending || destinations.length === 0}
            id="event-quantity"
            min="1"
            onChange={(event) => edit(setQuantity, event.currentTarget.value)}
            required
            step="1"
            type="number"
            value={quantity}
          />
          <fieldset className="destination-choices" disabled={isSending || destinations.length === 0}>
            <legend>Destinations</legend>
            {destinations.map((destination) => (
              <label className="destination-choice" key={destination.id}>
                <input
                  checked={destinationIds.includes(destination.id)}
                  disabled={isSending || destinations.length === 0}
                  onChange={() => toggleDestination(destination.id)}
                  type="checkbox"
                />
                <span>{destination.code}</span>
              </label>
            ))}
            {destinations.length === 0 ? (
              <p className="empty-state">Configure a destination before creating an event.</p>
            ) : null}
          </fieldset>

          {uncertain ? (
            <div className="uncertain-result" role="alert">
              <p>The response was not confirmed. The API may already have accepted this event.</p>
              <p>Retry sends the same serialized body and idempotency key. Editing a field starts a new event request with a new key.</p>
              <button className="secondary-button" disabled={isSending} type="submit">
                Retry same submission
              </button>
            </div>
          ) : null}
          {rejected ? (
            <p className="event-error" role="alert">
              {rejectedMessageValue} Edit a field to start a new request with a new key.
            </p>
          ) : null}
          {validationMessage ? <p className="event-error" role="alert">{validationMessage}</p> : null}
          {!uncertain ? (
            <button
              className="primary-button"
              disabled={isSending || rejected || destinations.length === 0}
              type="submit"
            >
              {isSending ? "Submitting event…" : "Submit event"}
            </button>
          ) : null}
        </form>
      )}
    </section>
  );
}

function isUncertain(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  const status = error.status;
  return status === null || status === 408 || status >= 500 || (status >= 200 && status < 300);
}

function rejectedMessage(error: unknown): string {
  if (error instanceof ApiError && error.status === 409) {
    return "The API reported a request conflict.";
  }
  return "The API rejected this event request.";
}
