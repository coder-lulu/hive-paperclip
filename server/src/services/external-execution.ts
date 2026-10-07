/** External execution identity comes from the run, never its agent's current adapter or a PID. */
export type ExternalExecutionRun = {
  id: string;
  companyId: string;
  driverKind?: string | null;
  status: string;
};

export type ExternalCancellationMetadata = {
  errorCode?: string;
  resultJson?: Record<string, unknown>;
  eventMessage?: string;
  eventPayload?: Record<string, unknown>;
  suppressImmediateRecovery?: boolean;
};

export type ExternalExecutionIntent = {
  kind: "cancel" | "drain";
  reason: string;
  cancellation?: ExternalCancellationMetadata;
};

export type ExternalExecutionPort = {
  /** Reattach an observer to the original durable binding; this must not start a provider. */
  recover(run: ExternalExecutionRun): Promise<void>;
  /** Deliver persisted cancellation; only validated Runtime evidence may settle the run. */
  cancel(run: ExternalExecutionRun, reason: string): Promise<void>;
  /** Relinquish delivery ownership without cancelling or releasing the execution slot. */
  drain(run: ExternalExecutionRun, reason: string): Promise<void>;
};

export type ExternalExecutionLifecycleOptions = {
  persistIntent(run: ExternalExecutionRun, intent: ExternalExecutionIntent): Promise<void>;
  port?: ExternalExecutionPort;
  onUnavailable?(run: ExternalExecutionRun, action: "recover" | "cancel" | "drain"): void;
};

export function isExternalExecutionRun(run: { driverKind?: string | null }): boolean {
  return run.driverKind === "hive_runtime";
}

/** Local cleanup never derives external termination from delivery expiry, task status or shutdown. */
export function createExternalExecutionLifecycle(options: ExternalExecutionLifecycleOptions) {
  const flights = new Map<string, Promise<boolean>>();
  const handle = (run: ExternalExecutionRun, action: "recover" | "cancel" | "drain", reason = "", cancellation?: ExternalCancellationMetadata) => {
    if (!isExternalExecutionRun(run)) { return Promise.resolve(false); }
    if (!["queued", "running", "scheduled_retry"].includes(run.status)) { return Promise.resolve(true); }
    const key = JSON.stringify([run.companyId, run.id, action]);
    const existing = flights.get(key);
    if (existing) { return existing; }
    const flight = (async () => {
      // A disconnected Runtime still receives the durable intent after controller restart.
      if (action !== "recover") { await options.persistIntent(run, {
        kind: action, reason, ...(action === "cancel" && cancellation ? { cancellation } : {}),
      }); }
      try {
        if (!options.port) { throw new Error("EXTERNAL_RUNTIME_UNAVAILABLE"); }
        await (action === "recover" ? options.port.recover(run) : options.port[action](run, reason));
      } catch {
        // Unavailable is a retained execution, never permission to retry, signal or terminalize it.
        options.onUnavailable?.(run, action);
      }
      return true;
    })().finally(() => {
      if (flights.get(key) === flight) { flights.delete(key); }
    });
    flights.set(key, flight);
    return flight;
  };
  return {
    recover: (run: ExternalExecutionRun) => handle(run, "recover"),
    cancel: (run: ExternalExecutionRun, reason: string, metadata?: ExternalCancellationMetadata) => handle(run, "cancel", reason, metadata),
    drain: (run: ExternalExecutionRun, reason: string) => handle(run, "drain", reason),
  };
}
