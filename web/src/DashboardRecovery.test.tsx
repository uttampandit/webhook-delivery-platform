import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import App from "./App";
import { EventBrowser } from "./events/EventBrowser";
import { session } from "./auth/memorySession";
const id = (n: number) =>
  `${String(n).padStart(8, "0")}-0000-4000-8000-000000000000`;
const item = (n: number) => ({
  id: id(n),
  type: "order.confirmed",
  created_at: new Date(Date.UTC(2026, 10, 18, 0, 0, n)).toISOString(),
  deliveries: [],
});
const range = (hi: number, lo: number) =>
  Array.from({ length: hi - lo + 1 }, (_, i) => hi - i);
const page = (ids: number[], cursor: string | null) =>
  new Response(JSON.stringify({ items: ids.map(item), next_cursor: cursor }), {
    status: 200,
  });
const drain = async () => {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
};
beforeEach(() => {
  vi.useFakeTimers();
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: "visible",
  });
});
afterEach(() => {
  cleanup();
  session.logout(null);
  vi.unstubAllGlobals();
  vi.clearAllTimers();
  vi.useRealTimers();
});
async function paginationSetup() {
  session.login("synthetic-test-token");
  let polls = 0;
  let finishOlder: ((r: Response) => void) | null = null;
  const fetchMock = vi.fn((input: RequestInfo | URL) => {
    const path = String(input);
    if (path.includes("cursor=after-21"))
      return new Promise<Response>((resolve) => {
        finishOlder = resolve;
      });
    if (path.includes("cursor=after-41"))
      return Promise.resolve(page(range(40, 21), "after-21"));
    if (path.startsWith("/v1/events/")) {
      const n = Number(path.split("/").pop()?.slice(0, 8));
      return Promise.resolve(
        new Response(
          JSON.stringify({ ...item(n), payload: {}, deliveries: [] }),
          { status: 200 },
        ),
      );
    }
    polls++;
    return Promise.resolve(
      polls === 1
        ? page(range(40, 21), "after-21")
        : page(range(60, 41), "after-41"),
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  render(<EventBrowser />);
  await drain();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(2000);
  });
  return {
    finish: async () => {
      await act(async () => {
        if (!finishOlder) throw Error("older request never started");
        finishOlder(page(range(20, 1), null));
      });
    },
  };
}
const visibleIds = () =>
  screen
    .getAllByRole("button", { name: /View order.confirmed/ })
    .map((e) => e.getAttribute("aria-label"));
it("control: resetting before starting pagination reaches all three 20-event windows", async () => {
  const pending = await paginationSetup();
  fireEvent.click(screen.getByRole("button", { name: "Show new events" }));
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
  });
  expect(visibleIds()).toHaveLength(40);
  fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
  await pending.finish();
  expect(visibleIds()).toHaveLength(60);
  expect(
    screen.queryByRole("button", { name: "Load older events" }),
  ).toBeNull();
});
it("discards stale pagination results and retains all intervening events", async () => {
  const pending = await paginationSetup();
  fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
  fireEvent.click(screen.getByRole("button", { name: "Show new events" }));
  await pending.finish();
  expect(visibleIds()).toHaveLength(20);
  expect(visibleIds()).toEqual(
    range(60, 41).map(
      (n) => `View order.confirmed, event ${id(n).slice(0, 8)}`,
    ),
  );
  await act(async () => {
    fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
  });
  expect(visibleIds()).toHaveLength(40);
  fireEvent.click(screen.getByRole("button", { name: "Load older events" }));
  await pending.finish();
  expect(visibleIds()).toHaveLength(60);
  expect(new Set(visibleIds()).size).toBe(60);
});
async function connectAfterFailure() {
  let online = false;
  const fetchMock = vi.fn((input: RequestInfo | URL) =>
    !online
      ? Promise.reject(new TypeError("offline"))
      : Promise.resolve(
          String(input) === "/v1/destinations"
            ? new Response('{"items":[]}', { status: 200 })
            : page([], null),
        ),
  );
  vi.stubGlobal("fetch", fetchMock);
  render(<App />);
  fireEvent.change(screen.getByLabelText("API token"), {
    target: { value: "synthetic-test-token" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Connect" }));
  await drain();
  expect(screen.getByText(/API is unavailable/)).toBeTruthy();
  online = true;
  return fetchMock;
}
it("control: restored connection succeeds after logout and reconnect", async () => {
  const fetchMock = await connectAfterFailure();
  fireEvent.click(screen.getByRole("button", { name: "Log out" }));
  fireEvent.change(screen.getByLabelText("API token"), {
    target: { value: "synthetic-test-token" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Connect" }));
  await drain();
  expect(screen.getByText("Connected to the API.")).toBeTruthy();
  expect(
    fetchMock.mock.calls.filter(([p]) => String(p) === "/v1/destinations"),
  ).toHaveLength(2);
});
it("retries a failed initial connection without logging out", async () => {
  const fetchMock = await connectAfterFailure();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60000);
  });
  expect(
    fetchMock.mock.calls.filter(([p]) => String(p) === "/v1/destinations"),
  ).toHaveLength(1);
  fireEvent.click(screen.getByRole("button", { name: "Retry connection" }));
  await drain();
  expect(screen.getByText("Connected to the API.")).toBeTruthy();
  expect(
    fetchMock.mock.calls.filter(([p]) => String(p) === "/v1/destinations"),
  ).toHaveLength(2);
  expect(session.getSnapshot().authenticated).toBe(true);
});
async function openPayload(payload: string) {
  session.login("synthetic-test-token");
  const raw = `{"id":"${id(1)}","type":"order.confirmed","created_at":"2026-11-18T00:00:01Z","payload":${payload},"deliveries":[]}`;
  vi.stubGlobal(
    "fetch",
    vi.fn((input: RequestInfo | URL) =>
      Promise.resolve(
        String(input).startsWith("/v1/events/")
          ? new Response(raw, { status: 200 })
          : page([1], null),
      ),
    ),
  );
  render(<EventBrowser />);
  await drain();
  fireEvent.click(
    screen.getByRole("button", {
      name: "View order.confirmed, event 00000001",
    }),
  );
  await drain();
  return document.querySelector(".payload-code")?.textContent;
}
it("control: exactly representable payload integer remains unchanged", async () => {
  expect(await openPayload('{"integer":9007199254740991}')).toContain(
    "9007199254740991",
  );
});
it("regression: large payload integer must remain unchanged", async () => {
  expect(await openPayload('{"integer":9007199254740993}')).toContain(
    "9007199254740993",
  );
});

it("preserves nested numeric lexemes and treats payload strings as text", async () => {
  const payload =
    '{"nested":{"payload":[9007199254740993,1e400,-0,1.2300]},"text":"<script>run()</script> {payload: []}","empty":{}}';
  const shown = await openPayload(payload);
  expect(shown).toContain("9007199254740993");
  expect(shown).toContain("1e400");
  expect(shown).toContain("-0");
  expect(shown).toContain("1.2300");
  expect(shown).toContain("<script>run()</script> {payload: []}");
  expect(shown).toContain('"empty": {}');
  expect(document.querySelector(".payload-code script")).toBeNull();
});
