import { describe, expect, it, vi } from "vitest";
import { ApiClient } from "./apiClient";
import { session } from "../auth/memorySession";

describe("ApiClient", () => {
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

    expect(fetch).toHaveBeenCalledWith(
      "/v1/destinations",
      expect.objectContaining({
        headers: { Authorization: "Bearer only-in-memory" },
        signal: expect.any(AbortSignal),
      }),
    );
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
    expect(fetch).toHaveBeenNthCalledWith(
      1,
      "/v1/events?limit=20&cursor=cursor%2B%2F%3D",
      expect.objectContaining({
        headers: { Authorization: "Bearer read-events-token" },
      }),
    );
    expect(fetch).toHaveBeenNthCalledWith(
      2,
      "/v1/events/00000000%2F0000",
      expect.objectContaining({
        headers: { Authorization: "Bearer read-events-token" },
      }),
    );
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
