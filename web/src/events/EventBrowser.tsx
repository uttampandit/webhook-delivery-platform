import { useEffect, useRef, useState } from "react";
import { ApiError, apiClient } from "../api/apiClient";
import type {
  AttemptDto,
  DestinationDto,
  EventDeliveryDetailDto,
  EventDetailDto,
  EventListDto,
  EventListItemDto,
  ReplayDto,
} from "../api/dto";
import { useVisiblePolling } from "../api/useVisiblePolling";
import { CreateEvent } from "./CreateEvent";
import { ReplayAction } from "./ReplayAction";

const PAGE_SIZE = 20;

export function EventBrowser({ destinations = [] }: { destinations?: DestinationDto[] }) {
  const [selectedEventId, setSelectedEventId] = useState<string | null>(null);

  return (
    <section aria-labelledby="events-heading" className="events-panel">
      <div className="events-heading-row">
        <div>
          <p className="eyebrow">Delivery history</p>
          <h2 id="events-heading">Events</h2>
        </div>
      </div>
      <div hidden={selectedEventId !== null}>
        <EventListView
          destinations={destinations}
          onSelect={setSelectedEventId}
          active={selectedEventId === null}
        />
      </div>
      {selectedEventId !== null ? (
        <EventDetailView
          eventId={selectedEventId}
          onBack={() => setSelectedEventId(null)}
        />
      ) : null}
    </section>
  );
}

function EventListView({
  onSelect,
  active,
  destinations,
}: {
  onSelect: (eventId: string) => void;
  active: boolean;
  destinations: DestinationDto[];
}) {
  const [events, setEvents] = useState<EventListItemDto[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [newEventsAvailable, setNewEventsAvailable] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const eventsRef = useRef(events);
  const hasFirstPage = useRef(false);
  const latestPage = useRef<EventListDto | null>(null);
  const requestLocked = useRef(false);
  const manualRequest = useRef<AbortController | null>(null);

  useEffect(
    () => () => {
      const controller = manualRequest.current;
      manualRequest.current = null;
      controller?.abort();
    },
    [],
  );

  function updateEvents(value: EventListItemDto[]): void {
    eventsRef.current = value;
    setEvents(value);
  }

  function acceptLatestPage(page: EventListDto, refreshedItems: EventListItemDto[]): void {
    latestPage.current = page;
    setLoading(false);
    setErrorMessage("");
    if (!hasFirstPage.current) {
      hasFirstPage.current = true;
      setNextCursor(page.next_cursor);
      updateEvents(page.items);
      return;
    }

    const visibleIds = new Set(eventsRef.current.map((event) => event.id));
    if (page.items.some((event) => !visibleIds.has(event.id))) {
      setNewEventsAvailable(true);
    }
    const freshItems = new Map(refreshedItems.map((event) => [event.id, event]));
    updateEvents(
      eventsRef.current.map((event) => freshItems.get(event.id) ?? event),
    );
  }

  const pollingStatus = useVisiblePolling(
    async (signal) => {
      if (requestLocked.current) return;
      requestLocked.current = true;
      try {
        const page = await apiClient.getEvents(PAGE_SIZE, undefined, signal);
        const refreshed = await refreshDisplayedEvents(page.items, eventsRef.current, signal);
        if (!signal.aborted) acceptLatestPage(page, refreshed);
      } catch (error) {
        if (signal.aborted) return;
        setLoading(false);
        setErrorMessage(friendlyError(error));
        throw error;
      } finally {
        requestLocked.current = false;
      }
    },
    active,
  );

  async function loadOlder(): Promise<void> {
    if (nextCursor === null || loadingOlder || requestLocked.current) return;
    requestLocked.current = true;
    setLoadingOlder(true);
    setErrorMessage("");
    const controller = new AbortController();
    manualRequest.current = controller;
    try {
      const page = await apiClient.getEvents(PAGE_SIZE, nextCursor, controller.signal);
      const currentIds = new Set(eventsRef.current.map((event) => event.id));
      const appended = page.items.filter((event) => !currentIds.has(event.id));
      updateEvents([...eventsRef.current, ...appended]);
      setNextCursor(page.next_cursor);
    } catch (error) {
      if (!controller.signal.aborted) setErrorMessage(friendlyError(error));
    } finally {
      if (manualRequest.current === controller) {
        requestLocked.current = false;
        manualRequest.current = null;
        setLoadingOlder(false);
      }
    }
  }

  function showLatestPage(): void {
    const page = latestPage.current;
    if (page === null) return;
    hasFirstPage.current = true;
    updateEvents(page.items);
    setNextCursor(page.next_cursor);
    setNewEventsAvailable(false);
    setErrorMessage("");
  }

  const liveStatus = pollingStatus === "disconnected"
    ? "Live updates are interrupted. The dashboard will retry while this page is visible."
    : "";

  return (
    <div className="event-list-view">
      <CreateEvent destinations={destinations} onOpenEvent={onSelect} />
      <p aria-live="polite" className="event-live-status" role="status">
        {loading ? "Loading events…" : liveStatus}
      </p>
      {errorMessage ? (
        <p className="event-error" role="alert">
          {events.length > 0
            ? `Could not refresh events. Showing the last successful results. ${errorMessage}`
            : errorMessage}
        </p>
      ) : null}
      {newEventsAvailable ? (
        <div className="new-events-notice">
          <p>New events are available. The current page order is unchanged.</p>
          <button className="secondary-button" onClick={showLatestPage} type="button">
            Show new events
          </button>
        </div>
      ) : null}
      {!loading && events.length === 0 && !errorMessage ? (
        <p className="empty-state">No events have been accepted yet.</p>
      ) : null}
      {events.length > 0 ? (
        <ol aria-label="Events, newest first" className="event-list">
          {events.map((event) => (
            <li className="event-card" key={event.id}>
              <button
                aria-label={`View ${event.type}, event ${shortId(event.id)}`}
                className="event-select"
                onClick={() => onSelect(event.id)}
                type="button"
              >
                <span className="event-type">{event.type}</span>
                <time dateTime={event.created_at}>{formatTime(event.created_at)}</time>
                <code>…{shortId(event.id)}</code>
              </button>
              <ul aria-label="Independent destination statuses" className="delivery-pills">
                {event.deliveries.map((delivery) => (
                  <li className="delivery-pill" key={delivery.id}>
                    <span>{delivery.destination_code}: {deliveryStatusLabel(delivery.status)}</span>
                    {delivery.has_unconfirmed_attempts ? (
                      <span className="uncertainty-badge">Earlier effect uncertain</span>
                    ) : null}
                  </li>
                ))}
              </ul>
            </li>
          ))}
        </ol>
      ) : null}
      {nextCursor !== null ? (
        <button
          className="secondary-button load-older-button"
          disabled={loadingOlder || pollingStatus === "polling"}
          onClick={() => void loadOlder()}
          type="button"
        >
          {loadingOlder ? "Loading older events…" : "Load older events"}
        </button>
      ) : null}
    </div>
  );
}

// The list endpoint refreshes the newest window. Use existing detail reads for
// displayed rows outside it, retaining only summaries and at most four active
// requests. This keeps paging positions stable even after many new arrivals.
async function refreshDisplayedEvents(
  newest: EventListItemDto[],
  displayed: EventListItemDto[],
  signal: AbortSignal,
): Promise<EventListItemDto[]> {
  const refreshed = [...newest];
  const newestIds = new Set(newest.map((event) => event.id));
  const missing = displayed.filter((event) => !newestIds.has(event.id));
  let failed = false;
  let failure: unknown;
  await Promise.all(Array.from({ length: Math.min(4, missing.length) }, async () => {
    while (!signal.aborted && !failed) {
      const event = missing.shift();
      if (!event) return;
      try {
        const detail = await apiClient.getEvent(event.id, signal);
        refreshed.push({
          ...event,
          deliveries: detail.deliveries.map((delivery) => ({
            id: delivery.id,
            destination_code: delivery.destination_code,
            status: delivery.status,
            has_unconfirmed_attempts: delivery.has_unconfirmed_attempts,
          })),
        });
      } catch (error) {
        failed = true;
        failure = error;
      }
    }
  }));
  // All started reads have settled before the poll releases its request lock.
  if (failed) throw failure;
  return refreshed;
}

function EventDetailView({
  eventId,
  onBack,
}: {
  eventId: string;
  onBack: () => void;
}) {
  const [event, setEvent] = useState<EventDetailDto | null>(null);
  const [loading, setLoading] = useState(true);
  const [errorMessage, setErrorMessage] = useState("");
  const [refreshAfterConflict, setRefreshAfterConflict] = useState(false);
  const conflictRefresh = useRef<AbortController | null>(null);

  useEffect(
    () => () => {
      const controller = conflictRefresh.current;
      conflictRefresh.current = null;
      controller?.abort();
    },
    [],
  );

  useEffect(() => {
    if (!refreshAfterConflict) return;
    const controller = new AbortController();
    conflictRefresh.current = controller;
    setErrorMessage("");
    void apiClient
      .getEvent(eventId, controller.signal)
      .then((result) => {
        if (controller.signal.aborted) return;
        setEvent(result);
        setLoading(false);
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        setErrorMessage(friendlyError(error));
      })
      .finally(() => {
        if (conflictRefresh.current !== controller) return;
        conflictRefresh.current = null;
        setRefreshAfterConflict(false);
      });
    return () => controller.abort();
  }, [eventId, refreshAfterConflict]);

  function refreshCurrentStatus(): void {
    if (!refreshAfterConflict) setRefreshAfterConflict(true);
  }

  const pollingStatus = useVisiblePolling(
    async (signal) => {
      try {
        const result = await apiClient.getEvent(eventId, signal);
        setEvent(result);
        setLoading(false);
        setErrorMessage("");
      } catch (error) {
        if (signal.aborted) return;
        setLoading(false);
        setErrorMessage(friendlyError(error));
        throw error;
      }
    },
    !refreshAfterConflict,
  );

  return (
    <div className="event-detail-view">
      <button className="back-button" onClick={onBack} type="button">
        ← Back to events
      </button>
      <p aria-live="polite" className="event-live-status" role="status">
        {loading ? "Loading event details…" : null}
        {refreshAfterConflict ? "Delivery state changed; refreshing current status…" : null}
        {!loading && pollingStatus === "disconnected"
          ? "Connection interrupted. Showing the last successful detail while retrying."
          : null}
      </p>
      {errorMessage ? (
        <p className="event-error" role="alert">
          {event
            ? `Could not refresh event details. Showing the last successful response. ${errorMessage}`
            : errorMessage}
        </p>
      ) : null}
      {!loading && event === null && !errorMessage ? (
        <p className="empty-state">This event is unavailable.</p>
      ) : null}
      {event ? <EventDetail event={event} onReplayConflict={refreshCurrentStatus} /> : null}
    </div>
  );
}

function EventDetail({
  event,
  onReplayConflict,
}: {
  event: EventDetailDto;
  onReplayConflict: () => void;
}) {
  const formattedPayload = JSON.stringify(event.payload, null, 2) ?? "null";
  return (
    <article className="event-detail">
      <header className="detail-header">
        <div>
          <p className="eyebrow">Event detail</p>
          <h3>{event.type}</h3>
        </div>
        <time dateTime={event.created_at}>{formatTime(event.created_at)}</time>
      </header>
      <p className="event-id-line">Event ID <code>{event.id}</code></p>
      <aside className="uncertainty-note" aria-label="Delivery uncertainty notice">
        A receiver may apply an operation before a response is lost. Repeated requests
        can happen. This sender does not guarantee exactly-once effects. An HTTP
        failure also cannot prove that the receiver had no effect.
      </aside>
      <section aria-labelledby="payload-heading" className="payload-section">
        <h4 id="payload-heading">Payload</h4>
        <pre className="payload-code"><code>{formattedPayload}</code></pre>
      </section>
      <section aria-labelledby="deliveries-heading" className="deliveries-section">
        <h4 id="deliveries-heading">Destination delivery history</h4>
        {event.deliveries.length === 0 ? (
          <p className="empty-state">This event has no destination deliveries.</p>
        ) : (
          <div className="delivery-details">
            {event.deliveries.map((delivery) => (
              <DeliveryHistory
                delivery={delivery}
                key={delivery.id}
                onReplayConflict={onReplayConflict}
              />
            ))}
          </div>
        )}
      </section>
    </article>
  );
}

function DeliveryHistory({
  delivery,
  onReplayConflict,
}: {
  delivery: EventDeliveryDetailDto;
  onReplayConflict: () => void;
}) {
  const timeline = buildTimeline(delivery.attempts, delivery.replays);
  return (
    <section aria-labelledby={`delivery-${delivery.id}`} className="delivery-detail-card">
      <header className="delivery-detail-heading">
        <div>
          <h5 id={`delivery-${delivery.id}`}>{delivery.destination_code}</h5>
          <p className="delivery-status-line">
            <span className={`status-badge status-${classSlug(delivery.status)}`}>
              {deliveryStatusLabel(delivery.status)}
            </span>
            {delivery.has_unconfirmed_attempts ? (
              <span className="uncertainty-badge">
                Has unconfirmed attempts — effect may have occurred
              </span>
            ) : null}
          </p>
        </div>
        {delivery.terminal_reason ? (
          <p className="terminal-reason">Terminal reason: <code>{delivery.terminal_reason}</code></p>
        ) : null}
      </header>
      <ReplayAction delivery={delivery} onConflict={onReplayConflict} />
      <dl className="delivery-facts">
        <div><dt>Operation ID</dt><dd><code>{delivery.operation_id}</code></dd></div>
        <div><dt>Cycle</dt><dd>{delivery.cycle_no} · {delivery.cycle_attempt_count} attempts this cycle</dd></div>
        <div><dt>Lifetime attempts</dt><dd>{delivery.lifetime_attempt_count} · max {delivery.max_attempts} per cycle</dd></div>
        {delivery.next_attempt_at ? (
          <div><dt>Next attempt due</dt><dd><time dateTime={delivery.next_attempt_at}>{formatTime(delivery.next_attempt_at)}</time></dd></div>
        ) : null}
        {delivery.lease_until ? (
          <div><dt>Active lease until</dt><dd><time dateTime={delivery.lease_until}>{formatTime(delivery.lease_until)}</time></dd></div>
        ) : null}
        {delivery.confirmed_at ? (
          <div><dt>Confirmed at</dt><dd><time dateTime={delivery.confirmed_at}>{formatTime(delivery.confirmed_at)}</time></dd></div>
        ) : null}
      </dl>
      <h6 className="timeline-heading">Attempt and replay timeline</h6>
      {timeline.length === 0 ? (
        <p className="empty-state">No attempts or replay records yet.</p>
      ) : (
        <ol aria-label={`${delivery.destination_code} attempt and replay timeline`} className="timeline">
          {timeline.map((entry) => (
            <li className={`timeline-entry timeline-${entry.kind}`} key={entry.key}>
              <time dateTime={entry.at}>{formatTime(entry.at)}</time>
              {entry.kind === "attempt" ? (
                <AttemptSummary attempt={entry.value} />
              ) : (
                <ReplaySummary replay={entry.value} />
              )}
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}

function AttemptSummary({ attempt }: { attempt: AttemptDto }) {
  return (
    <div className="timeline-copy">
      <strong>Attempt {attempt.attempt_no} · cycle {attempt.cycle_no}, try {attempt.cycle_attempt_no}</strong>
      <span>{attemptOutcomeLabel(attempt.outcome)}</span>
      <span>
        {attempt.finished_at ? `Finished ${formatTime(attempt.finished_at)}` : "Still running"}
        {attempt.http_status !== null ? ` · HTTP ${attempt.http_status}` : ""}
      </span>
      {attempt.error_code ? <span>Redacted error code: <code>{attempt.error_code}</code></span> : null}
      {attempt.duration_ms !== null ? <span>Duration: {attempt.duration_ms} ms</span> : null}
      {attempt.retry_delay_ms !== null ? <span>Scheduled retry delay: {attempt.retry_delay_ms} ms</span> : null}
    </div>
  );
}

function ReplaySummary({ replay }: { replay: ReplayDto }) {
  return (
    <div className="timeline-copy">
      <strong>Replay opened cycle {replay.to_cycle}</strong>
      <span>From cycle {replay.from_cycle} · actor {replay.actor}</span>
    </div>
  );
}

type TimelineEntry =
  | { kind: "attempt"; key: string; at: string; value: AttemptDto }
  | { kind: "replay"; key: string; at: string; value: ReplayDto };

function buildTimeline(attempts: AttemptDto[], replays: ReplayDto[]): TimelineEntry[] {
  return [
    ...attempts.map((attempt) => ({
      kind: "attempt" as const,
      key: `attempt-${attempt.id}`,
      at: attempt.started_at,
      value: attempt,
    })),
    ...replays.map((replay) => ({
      kind: "replay" as const,
      key: `replay-${replay.id}`,
      at: replay.created_at,
      value: replay,
    })),
  ].sort((left, right) => {
    const delta = Date.parse(left.at) - Date.parse(right.at);
    if (delta !== 0) return delta;
    if (left.kind !== right.kind) return left.kind === "attempt" ? -1 : 1;
    if (left.kind === "attempt" && right.kind === "attempt") {
      return left.value.attempt_no - right.value.attempt_no;
    }
    if (left.kind === "replay" && right.kind === "replay") {
      return left.value.to_cycle - right.value.to_cycle;
    }
    return left.key.localeCompare(right.key);
  });
}

function deliveryStatusLabel(status: string): string {
  switch (status) {
    case "pending": return "Pending";
    case "in_flight": return "In progress";
    case "succeeded": return "Acknowledged";
    case "dead": return "Dead";
    default: return humanize(status);
  }
}

function attemptOutcomeLabel(outcome: string): string {
  switch (outcome) {
    case "running": return "Running — confirmation is pending";
    case "acknowledged": return "Acknowledged — receiver returned 2xx";
    case "http_failure": return "HTTP failure — receiver effect is unknown";
    case "unconfirmed": return "Unconfirmed — receiver effect may have occurred";
    case "local_failure": return "Local failure before confirmed delivery";
    default: return humanize(outcome);
  }
}

function friendlyError(error: unknown): string {
  if (error instanceof ApiError && error.status === 404) {
    return "The requested event is unavailable.";
  }
  if (error instanceof ApiError && error.status !== null) {
    return "The API could not load this information. Try again shortly.";
  }
  return "The API is unavailable. Check the local server and try again.";
}

function humanize(value: string): string {
  return value.replaceAll("_", " ");
}

function classSlug(value: string): string {
  return value.replace(/[^a-z0-9-]/gi, "-").toLowerCase();
}

function shortId(value: string): string {
  return value.slice(0, 8);
}

function formatTime(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? value : date.toLocaleString();
}
