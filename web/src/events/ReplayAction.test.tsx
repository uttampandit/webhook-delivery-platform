import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { EventDeliveryDetailDto } from "../api/dto";
import { session } from "../auth/memorySession";
import { ReplayAction } from "./ReplayAction";

function deadDelivery(): EventDeliveryDetailDto {
  return {
    id: "delivery-dead",
    destination_code: "notification",
    status: "dead",
    has_unconfirmed_attempts: true,
    operation_id: "stable-operation-id",
    cycle_no: 1,
    cycle_attempt_count: 5,
    lifetime_attempt_count: 5,
    max_attempts: 5,
    next_attempt_at: null,
    lease_until: null,
    confirmed_at: null,
    terminal_reason: "attempts_exhausted",
    attempts: [],
    replays: [],
  };
}

function replayResponse() {
  return new Response(JSON.stringify({
    replay_id: "replay-1",
    delivery_id: "delivery-dead",
    cycle_no: 2,
    deduplicated: false,
  }), { status: 202 });
}

describe("ReplayAction", () => {
  beforeEach(() => session.login("replay-test-token"));

  afterEach(() => {
    cleanup();
    session.logout(null);
    vi.unstubAllGlobals();
  });

  it("only appears for dead deliveries and prevents a rapid double click", async () => {
    let resolveResponse: ((response: Response) => void) | undefined;
    const fetchMock = vi.fn((_input: RequestInfo | URL, _init?: RequestInit) => new Promise<Response>((resolve) => {
      resolveResponse = resolve;
    }));
    vi.stubGlobal("fetch", fetchMock);
    const active = { ...deadDelivery(), status: "succeeded" };
    const { rerender } = render(<ReplayAction delivery={active} onConflict={vi.fn()} />);
    expect(screen.queryByRole("button", { name: "Replay delivery" })).toBeNull();

    rerender(<ReplayAction delivery={deadDelivery()} onConflict={vi.fn()} />);
    const button = screen.getByRole("button", { name: "Replay delivery" });
    fireEvent.click(button);
    fireEvent.click(button);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({ method: "POST", body: "{}" });
    expect(screen.getByText(/previous attempt may already have taken effect/)).toBeTruthy();

    await act(async () => resolveResponse?.(replayResponse()));
    expect(await screen.findByText("Replay scheduled for cycle 2.")).toBeTruthy();
    expect(screen.getByText("This confirms scheduling, not delivery.")).toBeTruthy();
  });

  it("reuses replay identity after uncertain response and calls refresh on 409", async () => {
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new TypeError("response lost"))
      .mockResolvedValueOnce(replayResponse())
      .mockResolvedValueOnce(new Response("{}", { status: 409 }));
    vi.stubGlobal("fetch", fetchMock);
    const onConflict = vi.fn();
    const { rerender } = render(<ReplayAction delivery={deadDelivery()} onConflict={onConflict} />);

    fireEvent.click(screen.getByRole("button", { name: "Replay delivery" }));
    expect(await screen.findByText(/response was not confirmed/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Retry same replay" }));
    expect(await screen.findByText("Replay scheduled for cycle 2.")).toBeTruthy();

    const firstRequest = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const retryRequest = fetchMock.mock.calls[1]?.[1] as RequestInit;
    expect(firstRequest.body).toBe(retryRequest.body);
    expect(firstRequest.body).toBe("{}");
    expect(new Headers(firstRequest.headers).get("Idempotency-Key")).toBe(
      new Headers(retryRequest.headers).get("Idempotency-Key"),
    );

    rerender(<ReplayAction delivery={{ ...deadDelivery(), cycle_no: 2 }} onConflict={onConflict} />);
    expect(screen.getByRole("button", { name: "Replay delivery" })).toBeTruthy();
    expect(onConflict).not.toHaveBeenCalled();
  });

  it("reports a 409 as a conflict and requests current-state refresh", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("{}", { status: 409 }));
    vi.stubGlobal("fetch", fetchMock);
    const onConflict = vi.fn();
    render(<ReplayAction delivery={deadDelivery()} onConflict={onConflict} />);

    fireEvent.click(screen.getByRole("button", { name: "Replay delivery" }));

    expect(await screen.findByText("The delivery state changed. Checking its current status.")).toBeTruthy();
    expect(onConflict).toHaveBeenCalledTimes(1);
    expect(screen.queryByText(/Replay scheduled for cycle/)).toBeNull();
  });
  it.each(["success", "failure"])("ignores a late %s after polling advances to another dead cycle", async (outcome) => {
    let resolveFirst!: (response: Response) => void;
    let rejectFirst!: (error: Error) => void;
    let resolveSecond!: (response: Response) => void;
    const fetchMock = vi.fn()
      .mockImplementationOnce(() => new Promise<Response>((resolve, reject) => {
        resolveFirst = resolve;
        rejectFirst = reject;
      }))
      .mockImplementationOnce(() => new Promise<Response>((resolve) => { resolveSecond = resolve; }));
    vi.stubGlobal("fetch", fetchMock);
    const onConflict = vi.fn();
    const { rerender } = render(<ReplayAction delivery={deadDelivery()} onConflict={onConflict} />);
    fireEvent.click(screen.getByRole("button", { name: "Replay delivery" }));
    const firstRequest = fetchMock.mock.calls[0]?.[1] as RequestInit;

    rerender(<ReplayAction delivery={{ ...deadDelivery(), cycle_no: 2 }} onConflict={onConflict} />);
    fireEvent.click(screen.getByRole("button", { name: "Replay delivery" }));
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(firstRequest.signal?.aborted).toBe(true);
    const secondRequest = fetchMock.mock.calls[1]?.[1] as RequestInit;
    expect(new Headers(firstRequest.headers).get("Idempotency-Key")).not.toBe(
      new Headers(secondRequest.headers).get("Idempotency-Key"),
    );

    await act(async () => {
      if (outcome === "success") resolveFirst(replayResponse());
      else rejectFirst(new TypeError("late response loss"));
    });
    expect(screen.queryByText("Replay scheduled for cycle 2.")).toBeNull();
    expect(screen.queryByText(/response was not confirmed/)).toBeNull();
    const pending = screen.getByRole("button", { name: "Scheduling replay…" });
    expect((pending as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(pending);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(onConflict).not.toHaveBeenCalled();

    await act(async () => resolveSecond(new Response(JSON.stringify({
      replay_id: "replay-2", delivery_id: "delivery-dead", cycle_no: 3, deduplicated: false,
    }), { status: 202 })));
    expect(screen.getByText("Replay scheduled for cycle 3.")).toBeTruthy();
  });

});
