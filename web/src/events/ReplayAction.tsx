import { useEffect, useRef, useState } from "react";
import { ApiError, apiClient } from "../api/apiClient";
import type { EventDeliveryDetailDto, ReplayResponseDto } from "../api/dto";
import { createRequestIdentity } from "./requestIdentity";

type ReplayState =
  | { kind: "sending"; identity: ReturnType<typeof createRequestIdentity> }
  | { kind: "uncertain"; identity: ReturnType<typeof createRequestIdentity> }
  | { kind: "scheduled"; response: ReplayResponseDto }
  | { kind: "conflict"; message: string }
  | { kind: "error"; message: string };

export function ReplayAction({
  delivery,
  onConflict,
}: {
  delivery: EventDeliveryDetailDto;
  onConflict: () => void;
}) {
  const [action, setAction] = useState<ReplayState | null>(null);
  const cycleRef = useRef(delivery.cycle_no);
  const sendingRef = useRef(false);
  const controllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (cycleRef.current === delivery.cycle_no) return;
    cycleRef.current = delivery.cycle_no;
    // Polling has observed a new cycle. Results from the old action must not
    // replace that newer state or keep a newly dead cycle from being replayed.
    const controller = controllerRef.current;
    controllerRef.current = null;
    controller?.abort();
    sendingRef.current = false;
    setAction(null);
  }, [delivery.cycle_no]);

  useEffect(
    () => () => {
      const controller = controllerRef.current;
      controllerRef.current = null;
      controller?.abort();
    },
    [],
  );

  async function replay(): Promise<void> {
    if (sendingRef.current || delivery.status !== "dead") return;
    const identity = action?.kind === "uncertain"
      ? action.identity
      : createRequestIdentity("{}");
    const controller = new AbortController();
    controllerRef.current = controller;
    sendingRef.current = true;
    setAction({ kind: "sending", identity });
    try {
      const response = await apiClient.replayDelivery(
        delivery.id,
        identity.key,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      setAction({ kind: "scheduled", response });
    } catch (error) {
      if (controller.signal.aborted) return;
      if (error instanceof ApiError && error.status === 409) {
        setAction({ kind: "conflict", message: "The delivery state changed. Checking its current status." });
        onConflict();
      } else if (isUncertain(error)) {
        setAction({ kind: "uncertain", identity });
      } else {
        setAction({ kind: "error", message: "The replay request was not accepted." });
      }
    } finally {
      if (controllerRef.current === controller) {
        controllerRef.current = null;
        sendingRef.current = false;
      }
    }
  }

  if (delivery.status !== "dead") return null;
  if (action?.kind === "scheduled") {
    return (
      <div className="replay-result" role="status">
        <strong>Replay scheduled for cycle {action.response.cycle_no}.</strong>
        <span>This confirms scheduling, not delivery.</span>
        {action.response.deduplicated ? <span>This request was already recorded.</span> : null}
      </div>
    );
  }

  return (
    <section aria-label={`Replay ${delivery.destination_code} delivery`} className="replay-action">
      <p className="replay-consequence">
        Replay schedules the same operation again. A previous attempt may already have taken effect.
        Receiver deduplication is required to avoid duplicate effects.
      </p>
      {action?.kind === "uncertain" ? (
        <p className="uncertain-result" role="alert">
          The replay response was not confirmed. The request may already be recorded; retry uses the same key.
        </p>
      ) : null}
      {action?.kind === "conflict" ? (
        <p className="event-error" role="status">{action.message}</p>
      ) : null}
      {action?.kind === "error" ? (
        <p className="event-error" role="alert">{action.message}</p>
      ) : null}
      <button
        className="secondary-button"
        disabled={action?.kind === "sending"}
        onClick={() => void replay()}
        type="button"
      >
        {action?.kind === "sending"
          ? "Scheduling replay…"
          : action?.kind === "uncertain"
            ? "Retry same replay"
            : action?.kind === "conflict" || action?.kind === "error"
              ? "Replay if still dead"
              : "Replay delivery"}
      </button>
    </section>
  );
}

function isUncertain(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  const status = error.status;
  return status === null || status === 408 || status >= 500 || (status >= 200 && status < 300);
}
