import { describe, expect, it, vi } from "vitest";
import { MemorySession } from "./memorySession";

describe("MemorySession", () => {
  it("keeps authentication in the current instance and clears cached state and requests on logout", () => {
    const activeSession = new MemorySession();
    const clearCache = vi.fn();
    const unregisterClearer = activeSession.registerCacheClearer(clearCache);
    activeSession.login("secret-token");
    const request = activeSession.beginRequest();

    expect(activeSession.getSnapshot().authenticated).toBe(true);
    expect(request.signal.aborted).toBe(false);

    activeSession.logout();

    expect(activeSession.getSnapshot().authenticated).toBe(false);
    expect(activeSession.getSnapshot().notice).toBe("You have been signed out.");
    expect(request.signal.aborted).toBe(true);
    expect(clearCache).toHaveBeenCalledTimes(1);

    request.release();
    unregisterClearer();
    const refreshedSession = new MemorySession();
    expect(refreshedSession.getSnapshot().authenticated).toBe(false);
  });
});
