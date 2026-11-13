export type SessionSnapshot = {
  authenticated: boolean;
  notice: string | null;
  revision: number;
};

type RequestLease = {
  token: string;
  signal: AbortSignal;
  abort: () => void;
  release: () => void;
};

const signedOutSnapshot: SessionSnapshot = {
  authenticated: false,
  notice: null,
  revision: 0,
};

export class MemorySession {
  private token: string | null = null;
  private snapshot = signedOutSnapshot;
  private readonly listeners = new Set<() => void>();
  private readonly requests = new Set<AbortController>();
  private readonly cacheClearers = new Set<() => void>();

  getSnapshot = (): SessionSnapshot => this.snapshot;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  login(token: string): void {
    if (this.token !== null) {
      this.clearSensitiveState();
    }
    this.token = token;
    this.publish(null);
  }

  logout(notice: string | null = "You have been signed out."): void {
    this.token = null;
    this.clearSensitiveState();
    this.publish(notice);
  }

  registerCacheClearer(clear: () => void): () => void {
    this.cacheClearers.add(clear);
    return () => this.cacheClearers.delete(clear);
  }

  beginRequest(externalSignal?: AbortSignal): RequestLease {
    if (this.token === null) {
      throw new Error("An API token is required.");
    }

    const controller = new AbortController();
    const abortFromCaller = (): void => controller.abort(externalSignal?.reason);
    if (externalSignal?.aborted) {
      abortFromCaller();
    } else {
      externalSignal?.addEventListener("abort", abortFromCaller, { once: true });
    }
    this.requests.add(controller);

    let released = false;
    return {
      token: this.token,
      signal: controller.signal,
      abort: () => controller.abort(),
      release: () => {
        if (released) return;
        released = true;
        externalSignal?.removeEventListener("abort", abortFromCaller);
        this.requests.delete(controller);
      },
    };
  }

  private clearSensitiveState(): void {
    for (const controller of this.requests) controller.abort();
    this.requests.clear();
    for (const clear of this.cacheClearers) clear();
  }

  private publish(notice: string | null): void {
    this.snapshot = {
      authenticated: this.token !== null,
      notice,
      revision: this.snapshot.revision + 1,
    };
    for (const listener of this.listeners) listener();
  }
}

export const session = new MemorySession();
