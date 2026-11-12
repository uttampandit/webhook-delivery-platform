import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { EventDetailDto, EventListItemDto } from "../api/dto";
import { session } from "../auth/memorySession";
import { EventBrowser } from "./EventBrowser";

const sharedTime = "2026-11-12T10:00:00.000Z";
const id9 = "90000000-0000-4000-8000-000000000009";
const id8 = "80000000-0000-4000-8000-000000000008";
const id7 = "70000000-0000-4000-8000-000000000007";
const idA = "a0000000-0000-4000-8000-000000000004";

function listItem(id: string, status = "pending", uncertain = false): EventListItemDto {
  return {
    id,
    type: "order.confirmed",
    created_at: sharedTime,
    deliveries: [
      {
        id: `d-${id}`,
        destination_code: "fulfillment",
        status,
        has_unconfirmed_attempts: uncertain,
      },
    ],
  };
}

function listPage(items: EventListItemDto[], nextCursor: string | null) {
  return { items, next_cursor: nextCursor };
}

function detailFixture(): EventDetailDto {
  const payloadText = '<img src=x onerror="alert(1)"><script>run()</script>';
  return {
    id: id9,
    type: "order.confirmed",
    created_at: sharedTime,
    payload: { note: payloadText, quantity: 1 },
    deliveries: [
      {
        id: "delivery-fulfillment",
        operation_id: "operation-fulfillment-stable-id",
        destination_code: "fulfillment",
        status: "succeeded",
        cycle_no: 1,
        cycle_attempt_count: 2,
        lifetime_attempt_count: 2,
        max_attempts: 5,
        next_attempt_at: null,
        lease_until: null,
        confirmed_at: "2026-11-12T10:02:00.000Z",
        terminal_reason: null,
        has_unconfirmed_attempts: true,
        attempts: [
          {
            id: "attempt-1",
            attempt_no: 1,
            cycle_no: 1,
            cycle_attempt_no: 1,
            outcome: "unconfirmed",
            started_at: "2026-11-12T10:00:01.000Z",
            finished_at: "2026-11-12T10:00:03.000Z",
            http_status: null,
            error_code: "network_error",
            duration_ms: 2_000,
            retry_delay_ms: 1_000,
          },
          {
            id: "attempt-2",
            attempt_no: 2,
            cycle_no: 1,
            cycle_attempt_no: 2,
            outcome: "acknowledged",
            started_at: "2026-11-12T10:02:00.000Z",
            finished_at: "2026-11-12T10:02:01.000Z",
            http_status: 204,
            error_code: null,
            duration_ms: 1_000,
            retry_delay_ms: null,
          },
        ],
        replays: [
          {
            id: "replay-1",
            from_cycle: 1,
            to_cycle: 2,
            actor: "local-operator",
            created_at: "2026-11-12T10:01:00.000Z",
          },
        ],
      },
      {
        id: "delivery-notification",
        operation_id: "operation-notification-stable-id",
        destination_code: "notification",
        status: "dead",
        cycle_no: 1,
        cycle_attempt_count: 1,
        lifetime_attempt_count: 1,
        max_attempts: 5,
        next_attempt_at: null,
        lease_until: null,
        confirmed_at: null,
        terminal_reason: "attempts_exhausted",
        has_unconfirmed_attempts: false,
        attempts: [
          {
            id: "attempt-3",
            attempt_no: 1,
            cycle_no: 1,
            cycle_attempt_no: 1,
            outcome: "http_failure",
            started_at: "2026-11-12T10:00:02.000Z",
            finished_at: "2026-11-12T10:00:04.000Z",
            http_status: 503,
            error_code: "http_failure",
            duration_ms: 2_000,
            retry_delay_ms: null,
          },
        ],
        replays: [],
      },
    ],
  };
}

function setVisible(): void {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
}

describe("EventBrowser", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    setVisible();
    session.login("event-test-token");
  });

  afterEach(() => {
    cleanup();
    session.logout(null);
    vi.unstubAllGlobals();
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it("keeps equal-timestamp cursor pages stable and offers a deliberate refresh for new events", async () => {
    const initial = [listItem(id9), listItem(id8)];
    const updated = [listItem(idA), listItem(id9, "succeeded", true), listItem(id8)];
    const fetchMock = vi.fn((input: RequestInfo | URL) => {
      const path = String(input);
      if (path.includes("cursor=")) {
        return Promise.resolve(new Response(JSON.stringify(listPage([listItem(id7)], null)), { status: 200 }));
      }
      if (fetchMock.mock.calls.length === 3) {
        return Promise.resolve(new Response(JSON.stringify(listPage(updated, "next-new")), { status: 200 }));
      }
      return Promise.resolve(new Response(JSON.stringify(listPage(initial, "cursor-page-1")), { status: 200 }));
    });
    vi.stubGlobal(
      "fetch",
      fetchMock,
    );
    render(<EventBrowser />);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByRole("button", { name: `View order.confirmed, event ${id9.slice(0, 8)}` })).toBeTruthy();
    await act(async () => {
      fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByRole("button", { name: `View order.confirmed, event ${id7.slice(0, 8)}` })).toBeTruthy();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_000);
    });
    expect(screen.getByText("New events are available. The current page order is unchanged.")).toBeTruthy();
    expect(screen.queryByRole("button", { name: `View order.confirmed, event ${idA.slice(0, 8)}` })).toBeNull();
    const beforeRefresh = screen.getAllByRole("button", { name: /View order.confirmed/ });
    expect(beforeRefresh.map((button) => button.getAttribute("aria-label"))).toEqual([
      `View order.confirmed, event ${id9.slice(0, 8)}`,
      `View order.confirmed, event ${id8.slice(0, 8)}`,
      `View order.confirmed, event ${id7.slice(0, 8)}`,
    ]);
    expect(screen.getByText("Earlier effect uncertain")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "Show new events" }));
    const afterRefresh = screen.getAllByRole("button", { name: /View order.confirmed/ });
    expect(afterRefresh.map((button) => button.getAttribute("aria-label"))).toEqual([
      `View order.confirmed, event ${idA.slice(0, 8)}`,
      `View order.confirmed, event ${id9.slice(0, 8)}`,
      `View order.confirmed, event ${id8.slice(0, 8)}`,
    ]);
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/events?limit=20&cursor=cursor-page-1",
      expect.objectContaining({ headers: { Authorization: "Bearer event-test-token" } }),
    );
  });

  it("shows delivery history and uncertainty with escaped payload text", async () => {
    const page = listPage([listItem(id9, "succeeded", true)], null);
    const detail = detailFixture();
    vi.stubGlobal(
      "fetch",
      vi.fn((input: RequestInfo | URL) => {
        const path = String(input);
        const body = path.includes("/v1/events/") ? detail : page;
        return Promise.resolve(new Response(JSON.stringify(body), { status: 200 }));
      }),
    );
    render(<EventBrowser />);
    expect(screen.getByText("Loading events…")).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    fireEvent.click(screen.getByRole("button", { name: `View order.confirmed, event ${id9.slice(0, 8)}` }));
    expect(screen.getByText("Loading event details…")).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });

    expect(screen.getByText("Acknowledged — receiver returned 2xx")).toBeTruthy();
    expect(screen.getByText("HTTP failure — receiver effect is unknown")).toBeTruthy();
    expect(screen.getByText("Replay opened cycle 2")).toBeTruthy();
    expect(screen.getByText("Has unconfirmed attempts — effect may have occurred")).toBeTruthy();
    expect(screen.getByText(/A receiver may apply an operation before a response is lost/)).toBeTruthy();
    expect(screen.getByText("operation-fulfillment-stable-id")).toBeTruthy();
    expect(
      screen.getByText((_, node) => node?.textContent === "Terminal reason: attempts_exhausted"),
    ).toBeTruthy();

    const payloadNode = document.querySelector(".payload-code");
    expect(payloadNode?.textContent).toContain("<img src=x onerror=");
    expect(payloadNode?.textContent).toContain("<script>run()</script>");
    expect(payloadNode?.querySelector("img")).toBeNull();
    expect(payloadNode?.querySelector("script")).toBeNull();
  });

  it("shows an explicit empty state and generic network error without fixture fallback", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn()
        .mockResolvedValueOnce(new Response(JSON.stringify(listPage([], null)), { status: 200 })),
    );
    const { unmount } = render(<EventBrowser />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByText("No events have been accepted yet.")).toBeTruthy();

    unmount();
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("raw connection stack")));
    render(<EventBrowser />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(screen.getByRole("alert").textContent).toBe(
      "The API is unavailable. Check the local server and try again.",
    );
    expect(screen.queryByText(/raw connection stack|internal stack secret/)).toBeNull();
  });
});
