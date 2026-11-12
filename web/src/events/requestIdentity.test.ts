import { describe, expect, it, vi } from "vitest";
import { createIdempotencyKey, createRequestIdentity } from "./requestIdentity";

describe("request identity", () => {
  it("keeps the exact serialized bytes beside one generated key", () => {
    const keyFactory = vi.fn(() => "stable-key");
    const body = '{"type":"order.confirmed", "payload":{"quantity":1}}';

    const identity = createRequestIdentity(body, keyFactory);

    expect(identity).toEqual({ body, key: "stable-key" });
    expect(keyFactory).toHaveBeenCalledTimes(1);
  });

  it("generates a printable UUID key", () => {
    const key = createIdempotencyKey();

    expect(key).toMatch(/^[0-9a-f-]{36}$/i);
    expect(key.length).toBeLessThanOrEqual(128);
    expect(key).not.toMatch(/\s/);
  });
});
