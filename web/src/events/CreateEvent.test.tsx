import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { session } from "../auth/memorySession";
import { CreateEvent } from "./CreateEvent";

const destinations = [
  { id: "dest-fulfillment", code: "fulfillment" },
  { id: "dest-notification", code: "notification" },
];

function acceptedResponse(eventId = "event-created") {
  return new Response(JSON.stringify({
    event_id: eventId,
    deduplicated: false,
    created_at: "2026-11-12T10:00:00Z",
    delivery_ids: ["delivery-a"],
  }), { status: 202 });
}

function submitForm(): void {
  const form = screen.getByRole("button", { name: "Submit event" }).closest("form");
  if (!form) throw new Error("Create event form was not rendered");
  fireEvent.submit(form);
}

describe("CreateEvent", () => {
  beforeEach(() => session.login("create-event-test-token"));

  afterEach(() => {
    cleanup();
    session.logout(null);
    vi.unstubAllGlobals();
  });

  it("retries exact request bytes and key after response loss, then gives an edit a new key", async () => {
    const user = userEvent.setup();
    const onOpenEvent = vi.fn();
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new TypeError("connection dropped"))
      .mockResolvedValueOnce(acceptedResponse());
    vi.stubGlobal("fetch", fetchMock);
    render(<CreateEvent destinations={destinations} onOpenEvent={onOpenEvent} />);

    await user.type(screen.getByLabelText("Existing scenario ID"), "scenario-blue");
    await user.type(screen.getByLabelText("Order ID"), "order-17");
    await user.click(screen.getByLabelText("fulfillment"));
    await user.click(screen.getByRole("button", { name: "Submit event" }));

    await screen.findByRole("button", { name: "Retry same submission" });
    const firstRequest = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const originalBody = firstRequest.body;
    const originalKey = new Headers(firstRequest.headers).get("Idempotency-Key");
    expect(originalBody).toBe(
      '{"type":"order.confirmed","payload":{"scenario_id":"scenario-blue","order_id":"order-17","quantity":1},"destination_ids":["dest-fulfillment"]}',
    );
    expect(originalKey).toMatch(/^[0-9a-f-]{36}$/i);

    await user.click(screen.getByRole("button", { name: "Retry same submission" }));
    await screen.findByText("Accepted for processing.");
    const retryRequest = fetchMock.mock.calls[1]?.[1] as RequestInit;
    expect(retryRequest.body).toBe(originalBody);
    expect(new Headers(retryRequest.headers).get("Idempotency-Key")).toBe(originalKey);
    expect(screen.getByText("Delivery is not yet confirmed.")).toBeTruthy();

    await user.click(screen.getByRole("button", { name: "View event details" }));
    expect(onOpenEvent).toHaveBeenCalledWith("event-created");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("keeps the request identity when a success status has an unreadable response", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response("{}", { status: 202 }))
      .mockResolvedValueOnce(acceptedResponse());
    vi.stubGlobal("fetch", fetchMock);
    render(<CreateEvent destinations={destinations} onOpenEvent={vi.fn()} />);

    await user.type(screen.getByLabelText("Existing scenario ID"), "scenario-blue");
    await user.type(screen.getByLabelText("Order ID"), "order-19");
    await user.click(screen.getByLabelText("fulfillment"));
    await user.click(screen.getByRole("button", { name: "Submit event" }));
    await screen.findByRole("button", { name: "Retry same submission" });
    await user.click(screen.getByRole("button", { name: "Retry same submission" }));
    await screen.findByText("Accepted for processing.");

    const first = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const retry = fetchMock.mock.calls[1]?.[1] as RequestInit;
    expect(retry.body).toBe(first.body);
    expect(new Headers(retry.headers).get("Idempotency-Key")).toBe(
      new Headers(first.headers).get("Idempotency-Key"),
    );
  });

  it("validates scenario, quantity, and destination selection before any request", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    render(<CreateEvent destinations={destinations} onOpenEvent={vi.fn()} />);

    fireEvent.change(screen.getByLabelText("Existing scenario ID"), { target: { value: "scenario-blue" } });
    fireEvent.change(screen.getByLabelText("Order ID"), { target: { value: "order-17" } });
    fireEvent.change(screen.getByLabelText("Quantity"), { target: { value: "0" } });
    submitForm();
    expect(screen.getByText("Quantity must be a positive whole number.")).toBeTruthy();
    expect(fetchMock).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Quantity"), { target: { value: "1" } });
    submitForm();
    expect(screen.getByText("Choose one or both configured destinations.")).toBeTruthy();
    expect(fetchMock).not.toHaveBeenCalled();

    fireEvent.change(screen.getByLabelText("Existing scenario ID"), { target: { value: " " } });
    fireEvent.click(screen.getByLabelText("fulfillment"));
    submitForm();
    expect(screen.getByText("Enter both an existing scenario ID and an order ID.")).toBeTruthy();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("uses a fresh key when the operator edits an uncertain request", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new TypeError("connection dropped"))
      .mockResolvedValueOnce(acceptedResponse("event-edited"));
    vi.stubGlobal("fetch", fetchMock);
    render(<CreateEvent destinations={destinations} onOpenEvent={vi.fn()} />);

    await user.type(screen.getByLabelText("Existing scenario ID"), "scenario-blue");
    await user.type(screen.getByLabelText("Order ID"), "order-17");
    await user.click(screen.getByLabelText("fulfillment"));
    await user.click(screen.getByRole("button", { name: "Submit event" }));
    await screen.findByRole("button", { name: "Retry same submission" });

    await user.clear(screen.getByLabelText("Order ID"));
    await user.type(screen.getByLabelText("Order ID"), "order-18");
    await user.click(screen.getByRole("button", { name: "Submit event" }));
    await screen.findByText("Accepted for processing.");

    const first = fetchMock.mock.calls[0]?.[1] as RequestInit;
    const edited = fetchMock.mock.calls[1]?.[1] as RequestInit;
    expect(edited.body).not.toBe(first.body);
    expect(new Headers(edited.headers).get("Idempotency-Key")).not.toBe(
      new Headers(first.headers).get("Idempotency-Key"),
    );
    expect(JSON.stringify(screen.getByRole("status").textContent)).not.toContain("create-event-test-token");
  });
});
