import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import { session } from "./auth/memorySession";

describe("dashboard shell", () => {
  afterEach(() => {
    session.logout(null);
    vi.unstubAllGlobals();
  });

  it("connects with a password field and logs out with cached destinations cleared", async () => {
    const user = userEvent.setup();
    const storageMethods = ["getItem", "setItem", "removeItem", "clear"] as const;
    const storageSpies = storageMethods.map((method) => vi.spyOn(Storage.prototype, method));
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ items: [{ id: "dest-1", code: "orders" }] }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const { unmount } = render(<App />);

    const tokenField = screen.getByLabelText("API token");
    expect((tokenField as HTMLInputElement).type).toBe("password");
    await user.type(tokenField, "demo-token");
    await user.click(screen.getByRole("button", { name: "Connect" }));

    expect(await screen.findByText("orders", { selector: "li" })).toBeTruthy();
    expect(screen.queryByLabelText("API token")).toBeNull();
    expect(fetch).toHaveBeenCalledWith("/v1/destinations", expect.any(Object));
    const destinationsRequest = vi.mocked(fetch).mock.calls.find(([path]) => path === "/v1/destinations")?.[1];
    expect(new Headers(destinationsRequest?.headers).get("Authorization")).toBe("Bearer demo-token");
    for (const storageSpy of storageSpies) expect(storageSpy).not.toHaveBeenCalled();

    await user.click(screen.getByRole("button", { name: "Log out" }));
    expect((screen.getByLabelText("API token") as HTMLInputElement).value).toBe("");
    expect(screen.queryByText("orders")).toBeNull();
    expect(session.getSnapshot().authenticated).toBe(false);

    unmount();
    render(<App />);
    expect((screen.getByLabelText("API token") as HTMLInputElement).value).toBe("");
    expect(screen.queryByText("orders")).toBeNull();
  });

  it("requires token entry again when page module memory is recreated", async () => {
    session.login("old-page-token");

    vi.resetModules();
    const { session: refreshedSession } = await import("./auth/memorySession");

    expect(session.getSnapshot().authenticated).toBe(true);
    expect(refreshedSession.getSnapshot().authenticated).toBe(false);
  });

  it("returns to token entry and clears cached data when the API returns 401", async () => {
    const user = userEvent.setup();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("{}", { status: 401 })),
    );
    render(<App />);

    await user.type(screen.getByLabelText("API token"), "wrong-token");
    fireEvent.click(screen.getByRole("button", { name: "Connect" }));

    await waitFor(() => {
      expect(session.getSnapshot().authenticated).toBe(false);
    });
    expect(screen.getByLabelText("API token")).toBeTruthy();
    expect(screen.getByText("Your API token was rejected. Enter a valid token to continue.")).toBeTruthy();
  });
});
