import { afterEach, describe, expect, it, vi } from "vitest";
import { API_REQUEST_TIMEOUT_MS, ApiClient } from "./apiClient";
import { session } from "../auth/memorySession";

describe("ApiClient", () => {
  afterEach(() => {
    session.logout(null);
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it.each(["headers", "body"])("times out stalled response %s and releases its deadline", async (phase) => {
    vi.useFakeTimers();
    session.login("timeout-test-token");
    let requestSignal: AbortSignal | undefined;
    vi.stubGlobal("fetch", vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      requestSignal = init?.signal ?? undefined;
      const stalled = new Promise<never>((_resolve, reject) => {
        requestSignal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
      });
      return phase === "headers" ? stalled : Promise.resolve({ ok: true, status: 200, json: () => stalled });
    }));
    const pending = new ApiClient().getDestinations();
    const rejected = expect(pending).rejects.toMatchObject({ name: "ApiError", status: null });
    await vi.advanceTimersByTimeAsync(API_REQUEST_TIMEOUT_MS);
    await rejected;
    expect(requestSignal?.aborted).toBe(true);
    expect(session.getSnapshot().authenticated).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each(["headers", "body"])("preserves caller cancellation during %s and clears the request deadline", async (phase) => {
    vi.useFakeTimers();
    session.login("cancel-test-token");
    vi.stubGlobal("fetch", vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
      const stalled = new Promise<never>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
      });
      return phase === "headers" ? stalled : Promise.resolve({ ok: true, status: 200, json: () => stalled });
    }));
    const controller = new AbortController();
    const pending = new ApiClient().getDestinations(controller.signal);
    const rejected = expect(pending).rejects.toMatchObject({ name: "AbortError" });
    await Promise.resolve();
    controller.abort();
    await rejected;
    expect(vi.getTimerCount()).toBe(0);
  });

  it("sends the in-memory bearer token and returns destination DTOs", async () => {
    session.login("only-in-memory");
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ items: [{ id: "dest-1", code: "orders" }] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );

    const result = await new ApiClient().getDestinations();

    expect(fetch).toHaveBeenCalledWith("/v1/destinations", expect.any(Object));
    const request = vi.mocked(fetch).mock.calls[0]?.[1];
    expect(new Headers(request?.headers).get("Authorization")).toBe("Bearer only-in-memory");
    expect(request?.signal).toBeInstanceOf(AbortSignal);
    expect(result.items).toEqual([{ id: "dest-1", code: "orders" }]);
    expect(JSON.stringify(result)).not.toContain("only-in-memory");
  });

  it("uses cursor query parameters for event lists and an escaped path for detail", async () => {
    session.login("read-events-token");
    const eventList = {
      items: [
        {
          id: "90000000-0000-4000-8000-000000000003",
          type: "order.confirmed",
          created_at: "2026-11-12T10:00:00Z",
          deliveries: [
            {
              id: "91000000-0000-4000-8000-000000000003",
              destination_code: "fulfillment",
              status: "pending",
              has_unconfirmed_attempts: false,
            },
          ],
        },
      ],
      next_cursor: "cursor+/=",
    };
    const eventDetail = {
      id: eventList.items[0]?.id,
      type: "order.confirmed",
      created_at: "2026-11-12T10:00:00Z",
      payload: { order_id: "ord-3" },
      deliveries: [
        {
          ...eventList.items[0]?.deliveries[0],
          operation_id: "91000000-0000-4000-8000-000000000003",
          cycle_no: 1,
          cycle_attempt_count: 0,
          lifetime_attempt_count: 0,
          max_attempts: 5,
          next_attempt_at: null,
          lease_until: null,
          confirmed_at: null,
          terminal_reason: null,
          attempts: [],
          replays: [],
        },
      ],
    };
    vi.stubGlobal(
      "fetch",
      vi.fn()
        .mockResolvedValueOnce(new Response(JSON.stringify(eventList), { status: 200 }))
        .mockResolvedValueOnce(new Response(JSON.stringify(eventDetail), { status: 200 })),
    );

    const client = new ApiClient();
    const page = await client.getEvents(20, "cursor+/=");
    const detail = await client.getEvent("00000000/0000");

    expect(page.next_cursor).toBe("cursor+/=");
    expect(detail.id).toBe(eventList.items[0]?.id);
    expect(fetch).toHaveBeenNthCalledWith(1, "/v1/events?limit=20&cursor=cursor%2B%2F%3D", expect.any(Object));
    expect(fetch).toHaveBeenNthCalledWith(2, "/v1/events/00000000%2F0000", expect.any(Object));
    const firstRequest = vi.mocked(fetch).mock.calls[0]?.[1];
    const secondRequest = vi.mocked(fetch).mock.calls[1]?.[1];
    expect(new Headers(firstRequest?.headers).get("Authorization")).toBe("Bearer read-events-token");
    expect(new Headers(secondRequest?.headers).get("Authorization")).toBe("Bearer read-events-token");
  });

  it("sends exact event bytes and idempotency keys for create and replay", async () => {
    session.login("write-events-token");
    const createResult = {
      event_id: "created-event",
      deduplicated: false,
      created_at: "2026-11-12T11:00:00Z",
      delivery_ids: ["delivery-a"],
    };
    const replayResult = {
      replay_id: "replay-a",
      delivery_id: "delivery-a",
      cycle_no: 2,
      deduplicated: false,
    };
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(createResult), { status: 202 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(replayResult), { status: 202 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    const body = '{"type":"order.confirmed", "payload":{"quantity":2},"destination_ids":["destination-a"]}';

    await expect(client.createEvent(body, "create-key-1")).resolves.toEqual(createResult);
    await expect(client.replayDelivery("delivery/a", "replay-key-1")).resolves.toEqual(replayResult);

    const createRequest = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined;
    const replayRequest = fetchMock.mock.calls[1]?.[1] as RequestInit | undefined;
    expect(fetchMock.mock.calls[0]?.[0]).toBe("/v1/events");
    expect(createRequest?.method).toBe("POST");
    expect(createRequest?.body).toBe(body);
    expect(new Headers(createRequest?.headers).get("Idempotency-Key")).toBe("create-key-1");
    expect(new Headers(createRequest?.headers).get("Authorization")).toBe("Bearer write-events-token");
    expect(fetchMock.mock.calls[1]?.[0]).toBe("/v1/deliveries/delivery%2Fa/replay");
    expect(replayRequest?.body).toBe("{}");
    expect(new Headers(replayRequest?.headers).get("Idempotency-Key")).toBe("replay-key-1");
  });

  it("clears the token, pending requests, and registered sensitive caches after a 401", async () => {
    session.login("rejected-secret");
    const clearCache = vi.fn();
    const unregisterClearer = session.registerCacheClearer(clearCache);
    let firstSignal: AbortSignal | undefined;
    vi.stubGlobal(
      "fetch",
      vi.fn((_input: RequestInfo | URL, init?: RequestInit) => {
        if (firstSignal === undefined) {
          firstSignal = init?.signal ?? undefined;
          return new Promise<Response>((_resolve, reject) => {
            firstSignal?.addEventListener(
              "abort",
              () => reject(new DOMException("Aborted", "AbortError")),
              { once: true },
            );
          });
        }
        return Promise.resolve(new Response("{}", { status: 401 }));
      }),
    );

    const client = new ApiClient();
    const pending = client.getDestinations();
    const rejected = client.getDestinations();

    await expect(rejected).rejects.toMatchObject({ status: 401 });
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(session.getSnapshot().authenticated).toBe(false);
    expect(session.getSnapshot().notice).toBe(
      "Your API token was rejected. Enter a valid token to continue.",
    );
    expect(firstSignal?.aborted).toBe(true);
    expect(clearCache).toHaveBeenCalledTimes(1);
    unregisterClearer();
  });
});
