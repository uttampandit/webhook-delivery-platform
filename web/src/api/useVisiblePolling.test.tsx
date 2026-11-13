import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { API_REQUEST_TIMEOUT_MS, ApiClient } from "./apiClient";
import { session } from "../auth/memorySession";
import { useVisiblePolling } from "./useVisiblePolling";

function setVisibility(state: "visible" | "hidden"): void {
  Object.defineProperty(document, "visibilityState", {
    configurable: true,
    value: state,
  });
  document.dispatchEvent(new Event("visibilitychange"));
}

describe("useVisiblePolling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    setVisibility("visible");
  });

  afterEach(() => {
    session.logout(null);
    vi.unstubAllGlobals();
    setVisibility("visible");
    vi.useRealTimers();
  });

  it("runs while visible, aborts and pauses while hidden, then resumes", async () => {
    const signals: AbortSignal[] = [];
    const poll = vi.fn((signal: AbortSignal) => {
      signals.push(signal);
      if (signals.length > 1) return Promise.resolve();
      return new Promise<void>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => reject(new DOMException("Aborted", "AbortError")),
          { once: true },
        );
      });
    });
    const { result } = renderHook(() => useVisiblePolling(poll, true, 2_000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(poll).toHaveBeenCalledTimes(1);

    act(() => setVisibility("hidden"));
    expect(result.current).toBe("paused");
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(poll).toHaveBeenCalledTimes(1);
    expect(signals[0]?.aborted).toBe(true);

    act(() => setVisibility("visible"));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(poll).toHaveBeenCalledTimes(2);
  });

  it("aborts an in-flight poll and clears timers on unmount", async () => {
    let signal: AbortSignal | undefined;
    const poll = vi.fn((_signal: AbortSignal) => {
      signal = _signal;
      return new Promise<void>(() => undefined);
    });
    const { unmount } = renderHook(() => useVisiblePolling(poll, true, 2_000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(poll).toHaveBeenCalledTimes(1);
    unmount();

    expect(signal?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("clears the next polling timer on unmount", async () => {
    const poll = vi.fn(async () => undefined);
    const { unmount } = renderHook(() => useVisiblePolling(poll, true, 2_000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(poll).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    unmount();

    expect(vi.getTimerCount()).toBe(0);
  });

  it("backs off after failures without overlapping requests", async () => {
    let active = 0;
    let peak = 0;
    const poll = vi.fn(async () => {
      active += 1;
      peak = Math.max(peak, active);
      await Promise.resolve();
      active -= 1;
      throw new Error("offline");
    });
    renderHook(() => useVisiblePolling(poll, true, 2_000, 5_000));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(poll).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_000);
    });
    expect(poll).toHaveBeenCalledTimes(2);
    expect(peak).toBe(1);
    expect(vi.getTimerCount()).toBe(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_999);
    });
    expect(poll).toHaveBeenCalledTimes(2);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1);
    });
    expect(poll).toHaveBeenCalledTimes(3);
  });
  it("reports a timed-out API poll as disconnected and recovers on the next bounded retry", async () => {
    session.login("poll-timeout-token");
    const fetchMock = vi.fn()
      .mockImplementationOnce((_input: RequestInfo | URL, init?: RequestInit) => new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
      }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ items: [], next_cursor: null }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const client = new ApiClient();
    const { result, unmount } = renderHook(() => useVisiblePolling(async (signal) => {
      await client.getEvents(20, undefined, signal);
    }, true));
    await act(async () => { await vi.advanceTimersByTimeAsync(0); });
    expect(result.current).toBe("polling");
    await act(async () => { await vi.advanceTimersByTimeAsync(API_REQUEST_TIMEOUT_MS); });
    expect(result.current).toBe("disconnected");
    await act(async () => { await vi.advanceTimersByTimeAsync(4_000); });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result.current).toBe("connected");
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

});
