import { useEffect, useState, useSyncExternalStore } from "react";
import type { FormEvent } from "react";
import { apiClient } from "./api/apiClient";
import type { DestinationDto } from "./api/dto";
import { session } from "./auth/memorySession";

type LoadState = "idle" | "loading" | "loaded" | "error";

export default function App() {
  const sessionSnapshot = useSyncExternalStore(
    session.subscribe,
    session.getSnapshot,
    session.getSnapshot,
  );
  const [tokenInput, setTokenInput] = useState("");
  const [destinations, setDestinations] = useState<DestinationDto[]>([]);
  const [loadState, setLoadState] = useState<LoadState>("idle");
  const [loadMessage, setLoadMessage] = useState("");

  useEffect(() => {
    if (!sessionSnapshot.authenticated) {
      setDestinations([]);
      setLoadState("idle");
      setLoadMessage("");
      return;
    }

    const controller = new AbortController();
    let cancelled = false;
    setDestinations([]);
    setLoadState("loading");
    setLoadMessage("");
    const unregisterClearer = session.registerCacheClearer(() => {
      setDestinations([]);
      setLoadState("idle");
      setLoadMessage("");
    });

    void apiClient
      .getDestinations(controller.signal)
      .then(({ items }) => {
        if (cancelled) return;
        setDestinations(items);
        setLoadState("loaded");
      })
      .catch((error: unknown) => {
        if (cancelled || controller.signal.aborted) return;
        setLoadState("error");
        setLoadMessage(
          error instanceof Error
            ? error.message
            : "The API is unavailable. Check the local server and try again.",
        );
      });

    return () => {
      cancelled = true;
      controller.abort();
      unregisterClearer();
    };
  }, [sessionSnapshot]);

  function submitToken(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const token = tokenInput.trim();
    if (token.length === 0) return;
    session.login(token);
    setTokenInput("");
  }

  function logout(): void {
    setTokenInput("");
    session.logout();
  }

  return (
    <main className="shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">Local operator console</p>
          <h1>Webhook delivery</h1>
        </div>
        {sessionSnapshot.authenticated ? (
          <button className="secondary-button" onClick={logout} type="button">
            Log out
          </button>
        ) : null}
      </header>

      <p className="intro">
        Inspect delivery activity from your local webhook platform.
      </p>

      {sessionSnapshot.authenticated ? (
        <section aria-labelledby="workspace-heading" className="workspace">
          <h2 id="workspace-heading">Workspace</h2>
          <p aria-live="polite" className="status" role="status">
            {loadState === "loading" ? "Connecting to the API…" : null}
            {loadState === "loaded" ? "Connected to the API." : null}
            {loadState === "error" ? loadMessage : null}
          </p>
          {loadState === "loaded" ? (
            <section aria-labelledby="destinations-heading" className="destination-section">
              <h3 id="destinations-heading">Destinations</h3>
              {destinations.length === 0 ? (
                <p className="empty-state">No destinations are configured.</p>
              ) : (
                <ul className="destination-list">
                  {destinations.map((destination) => (
                    <li key={destination.id}>{destination.code}</li>
                  ))}
                </ul>
              )}
            </section>
          ) : null}
        </section>
      ) : (
        <section aria-labelledby="connect-heading" className="connect-panel">
          <h2 id="connect-heading">Connect to the local API</h2>
          <p className="supporting-copy">
            Enter the operator token configured for your local server. It stays in
            memory until you log out or refresh this page.
          </p>
          <form className="token-form" onSubmit={submitToken}>
            <label htmlFor="api-token">API token</label>
            <input
              autoComplete="off"
              id="api-token"
              name="api-token"
              onChange={(event) => setTokenInput(event.currentTarget.value)}
              required
              type="password"
              value={tokenInput}
            />
            <button className="primary-button" type="submit">
              Connect
            </button>
          </form>
          <p aria-live="polite" className="notice" role="status">
            {sessionSnapshot.notice}
          </p>
        </section>
      )}

      <footer className="footer-note">
        Local dashboard · API tokens are not saved between page loads
      </footer>
    </main>
  );
}
