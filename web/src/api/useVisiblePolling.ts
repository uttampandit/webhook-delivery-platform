import { useEffect, useRef, useState } from "react";

export type PollingStatus = "idle" | "paused" | "polling" | "connected" | "disconnected";
export type PollTask = (signal: AbortSignal) => Promise<void>;

export function useVisiblePolling(
  task: PollTask,
  enabled: boolean,
  intervalMs = 2_000,
  maxRetryDelayMs = 60_000,
): PollingStatus {
  const taskRef = useRef(task);
  const [status, setStatus] = useState<PollingStatus>(enabled ? "paused" : "idle");
  taskRef.current = task;

  useEffect(() => {
    if (!enabled) {
      setStatus("idle");
      return;
    }

    let disposed = false;
    let running = false;
    let restartAfterRun = false;
    let failures = 0;
    let timer: number | null = null;
    let activeController: AbortController | null = null;

    const visible = (): boolean => document.visibilityState === "visible";
    const clearTimer = (): void => {
      if (timer !== null) window.clearTimeout(timer);
      timer = null;
    };
    const schedule = (delayMs: number): void => {
      if (disposed || !visible()) {
        setStatus("paused");
        return;
      }
      clearTimer();
      timer = window.setTimeout(() => {
        timer = null;
        if (disposed || !visible()) {
          setStatus("paused");
          return;
        }
        if (running) {
          restartAfterRun = true;
          return;
        }
        running = true;
        activeController = new AbortController();
        const currentController = activeController;
        setStatus("polling");
        void Promise.resolve()
          .then(() => taskRef.current(currentController.signal))
          .then(() => {
            if (currentController.signal.aborted) return;
            failures = 0;
            setStatus("connected");
            schedule(intervalMs);
          })
          .catch(() => {
            if (currentController.signal.aborted) return;
            failures += 1;
            setStatus("disconnected");
            const exponent = Math.min(failures, 10);
            schedule(Math.min(maxRetryDelayMs, Math.max(1_000, intervalMs * 2 ** exponent)));
          })
          .finally(() => {
            running = false;
            if (activeController === currentController) activeController = null;
            if (disposed) return;
            if (!visible()) {
              setStatus("paused");
              return;
            }
            if (restartAfterRun) {
              restartAfterRun = false;
              schedule(0);
            }
          });
      }, delayMs);
    };
    const onVisibilityChange = (): void => {
      if (visible()) {
        if (running) restartAfterRun = true;
        else schedule(0);
      } else {
        clearTimer();
        restartAfterRun = false;
        activeController?.abort();
        setStatus("paused");
      }
    };

    document.addEventListener("visibilitychange", onVisibilityChange);
    if (visible()) schedule(0);
    else setStatus("paused");

    return () => {
      disposed = true;
      clearTimer();
      activeController?.abort();
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [enabled, intervalMs, maxRetryDelayMs]);

  return status;
}
