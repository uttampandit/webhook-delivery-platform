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
