import { session } from "../auth/memorySession";
import type {
  DestinationListDto,
  EventCreateResponseDto,
  EventDetailDto,
  EventListDto,
  ReplayResponseDto,
} from "./dto";

export const API_REQUEST_TIMEOUT_MS = 10_000;

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export class ApiClient {
  async getDestinations(signal?: AbortSignal): Promise<DestinationListDto> {
    return this.getJson("/v1/destinations", isDestinationList, signal);
  }

  async getEvents(
    limit = 20,
    cursor?: string,
    signal?: AbortSignal,
  ): Promise<EventListDto> {
    const query = new URLSearchParams({ limit: String(limit) });
    if (cursor) query.set("cursor", cursor);
    return this.getJson(`/v1/events?${query.toString()}`, isEventList, signal);
  }

  async getEvent(eventId: string, signal?: AbortSignal): Promise<EventDetailDto> {
    return this.getJson(`/v1/events/${encodeURIComponent(eventId)}`, isEventDetail, signal);
  }

  async createEvent(
    body: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ): Promise<EventCreateResponseDto> {
    return this.requestJson(
      "/v1/events",
      isEventCreateResponse,
      signal,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey,
        },
        body,
      },
    );
  }

  async replayDelivery(
    deliveryId: string,
    idempotencyKey: string,
    signal?: AbortSignal,
  ): Promise<ReplayResponseDto> {
    return this.requestJson(
      `/v1/deliveries/${encodeURIComponent(deliveryId)}/replay`,
      isReplayResponse,
      signal,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey,
        },
        body: "{}",
      },
    );
  }

  private async getJson<T>(
    path: string,
    validate: (value: unknown) => value is T,
    signal?: AbortSignal,
  ): Promise<T> {
    return this.requestJson(path, validate, signal, { method: "GET" });
  }

  private async requestJson<T>(
    path: string,
    validate: (value: unknown) => value is T,
    signal: AbortSignal | undefined,
    init: RequestInit,
  ): Promise<T> {
    const request = session.beginRequest(signal);
    let timedOut = false;
    const deadline = window.setTimeout(() => {
      timedOut = true;
      request.abort();
    }, API_REQUEST_TIMEOUT_MS);
    try {
      const headers = new Headers(init.headers);
      headers.set("Authorization", `Bearer ${request.token}`);
      const response = await fetch(path, {
        ...init,
        headers,
        signal: request.signal,
      });
      if (request.signal.aborted) {
        throw new DOMException("The request was aborted.", "AbortError");
      }
      if (response.status === 401) {
        session.logout("Your API token was rejected. Enter a valid token to continue.");
        throw new ApiError("Your API token was rejected.", 401);
      }
      if (!response.ok) {
        throw new ApiError("The API could not complete the request.", response.status);
      }

      let result: unknown;
      try {
        result = await response.json();
      } catch {
        if (request.signal.aborted) {
          throw new DOMException("The request was aborted.", "AbortError");
        }
        throw new ApiError("The API returned an invalid response.", response.status);
      }
      if (request.signal.aborted) {
        throw new DOMException("The request was aborted.", "AbortError");
      }
      if (!validate(result)) {
        throw new ApiError("The API returned an invalid response.", response.status);
      }
      return result;
    } catch (error) {
      // Caller/session cancellation remains an abort. A deadline is uncertain
      // network failure so write actions retain their original body and key.
      if (timedOut) throw new ApiError("The API request timed out. Try again.", null);
      if (error instanceof ApiError) throw error;
      if (request.signal.aborted || isAbortError(error)) throw error;
      throw new ApiError("The API is unavailable. Check the local server and try again.", null);
    } finally {
      window.clearTimeout(deadline);
      request.release();
    }
  }
}

function isEventCreateResponse(value: unknown): value is EventCreateResponseDto {
  return (
    isRecord(value) &&
    typeof value.event_id === "string" &&
    typeof value.deduplicated === "boolean" &&
    typeof value.created_at === "string" &&
    Array.isArray(value.delivery_ids) &&
    value.delivery_ids.every((id) => typeof id === "string")
  );
}

function isReplayResponse(value: unknown): value is ReplayResponseDto {
  return (
    isRecord(value) &&
    typeof value.replay_id === "string" &&
    typeof value.delivery_id === "string" &&
    typeof value.cycle_no === "number" &&
    typeof value.deduplicated === "boolean"
  );
}

function isDestinationList(value: unknown): value is DestinationListDto {
  if (typeof value !== "object" || value === null || !("items" in value)) return false;
  return (
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        typeof item === "object" &&
        item !== null &&
        "id" in item &&
        typeof item.id === "string" &&
        "code" in item &&
        typeof item.code === "string",
    )
  );
}

function isEventList(value: unknown): value is EventListDto {
  return (
    isRecord(value) &&
    Array.isArray(value.items) &&
    value.items.every(isEventListItem) &&
    (value.next_cursor === null || typeof value.next_cursor === "string")
  );
}

function isEventListItem(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.created_at === "string" &&
    Array.isArray(value.deliveries) &&
    value.deliveries.every(isDeliveryListItem)
  );
}

function isDeliveryListItem(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.destination_code === "string" &&
    typeof value.status === "string" &&
    typeof value.has_unconfirmed_attempts === "boolean"
  );
}

function isEventDetail(value: unknown): value is EventDetailDto {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.type === "string" &&
    typeof value.created_at === "string" &&
    "payload" in value &&
    Array.isArray(value.deliveries) &&
    value.deliveries.every(isDeliveryDetail)
  );
}

function isDeliveryDetail(value: unknown): boolean {
  return (
    isRecord(value) &&
    isDeliveryListItem(value) &&
    typeof value.operation_id === "string" &&
    typeof value.cycle_no === "number" &&
    typeof value.cycle_attempt_count === "number" &&
    typeof value.lifetime_attempt_count === "number" &&
    typeof value.max_attempts === "number" &&
    isNullableString(value.next_attempt_at) &&
    isNullableString(value.lease_until) &&
    isNullableString(value.confirmed_at) &&
    isNullableString(value.terminal_reason) &&
    Array.isArray(value.attempts) &&
    value.attempts.every(isAttempt) &&
    Array.isArray(value.replays) &&
    value.replays.every(isReplay)
  );
}

function isAttempt(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.attempt_no === "number" &&
    typeof value.cycle_no === "number" &&
    typeof value.cycle_attempt_no === "number" &&
    typeof value.outcome === "string" &&
    typeof value.started_at === "string" &&
    isNullableString(value.finished_at) &&
    isNullableNumber(value.http_status) &&
    isNullableString(value.error_code) &&
    isNullableNumber(value.duration_ms) &&
    isNullableNumber(value.retry_delay_ms)
  );
}

function isReplay(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.from_cycle === "number" &&
    typeof value.to_cycle === "number" &&
    typeof value.actor === "string" &&
    typeof value.created_at === "string"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isNullableNumber(value: unknown): value is number | null {
  return value === null || typeof value === "number";
}

function isAbortError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

export const apiClient = new ApiClient();
