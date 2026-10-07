import { describe, expect, it, vi } from "vitest";
import { createExternalExecutionLifecycle, isExternalExecutionRun } from "./external-execution.js";

const run = (patch = {}) => ({ id: "run:retained", companyId: "company:one", driverKind: "hive_runtime", status: "running", ...patch });

function ports() {
  return {
    persistIntent: vi.fn(async () => {}),
    port: { recover: vi.fn(async () => {}), cancel: vi.fn(async () => {}), drain: vi.fn(async () => {}) },
    onUnavailable: vi.fn(),
  };
}

describe("external execution lifecycle ownership", () => {
  it("uses persisted driver identity after the agent changes and without consulting a local PID", async () => {
    const options = ports();
    const retained = run({ adapterType: "codex_local", processPid: process.pid, updatedAt: new Date(0) });
    const lifecycle = createExternalExecutionLifecycle(options);
    expect(await lifecycle.recover(retained)).toBe(true);
    expect(options.port.recover).toHaveBeenCalledWith(retained);
    expect(options.persistIntent).not.toHaveBeenCalled();
    expect(options.port.cancel).not.toHaveBeenCalled();
    expect(retained.status).toBe("running");
  });

  it.each([null, undefined, "codex", "claude", "process"])("leaves driver %s to its existing lifecycle", async (driverKind) => {
    const options = ports();
    const lifecycle = createExternalExecutionLifecycle(options);
    expect(isExternalExecutionRun({ driverKind })).toBe(false);
    expect(await lifecycle.cancel(run({ driverKind }), "board_stop")).toBe(false);
    expect(options.persistIntent).not.toHaveBeenCalled();
    expect(options.port.cancel).not.toHaveBeenCalled();
  });

  it.each(["cancel", "drain"] as const)("persists %s before touching delivery ownership", async (action) => {
    const options = ports();
    const order: string[] = [];
    options.persistIntent.mockImplementation(async () => { order.push("persist"); });
    options.port[action].mockImplementation(async () => { order.push("deliver"); });
    expect(await createExternalExecutionLifecycle(options)[action](run(), "test_intent")).toBe(true);
    expect(order).toEqual(["persist", "deliver"]);
    expect(options.persistIntent).toHaveBeenCalledWith(run(), { kind: action, reason: "test_intent" });
  });

  it("does not deliver cancellation when its durable intent failed", async () => {
    const options = ports();
    options.persistIntent.mockRejectedValue(new Error("database unavailable"));
    await expect(createExternalExecutionLifecycle(options).cancel(run(), "board_stop")).rejects.toThrow("database unavailable");
    expect(options.port.cancel).not.toHaveBeenCalled();
  });

  it("retains unknown execution after observer loss and records an explicit unavailable state", async () => {
    const options = ports();
    options.port.recover.mockRejectedValue(new Error("Runtime unavailable"));
    const retained = run();
    expect(await createExternalExecutionLifecycle(options).recover(retained)).toBe(true);
    expect(options.onUnavailable).toHaveBeenCalledWith(retained, "recover");
    expect(retained.status).toBe("running");
    expect(options.port.cancel).not.toHaveBeenCalled();
    expect(options.port.drain).not.toHaveBeenCalled();
  });

  it("retains cancellation intent even when no Runtime transport is configured", async () => {
    const options = ports();
    const lifecycle = createExternalExecutionLifecycle({ ...options, port: undefined });
    expect(await lifecycle.cancel(run(), "budget_pause")).toBe(true);
    expect(options.persistIntent).toHaveBeenCalledOnce();
    expect(options.onUnavailable).toHaveBeenCalledWith(run(), "cancel");
  });

  it("coalesces concurrent recovery for the same company/run without mixing companies", async () => {
    const options = ports();
    let finish!: () => void;
    options.port.recover.mockImplementation(() => new Promise<void>((resolve) => { finish = resolve; }));
    const lifecycle = createExternalExecutionLifecycle(options);
    const first = lifecycle.recover(run());
    const second = lifecycle.recover(run());
    expect(first).toBe(second);
    expect(options.port.recover).toHaveBeenCalledOnce();
    finish();
    await first;
    options.port.recover.mockResolvedValue(undefined);
    await lifecycle.recover(run({ companyId: "company:other" }));
    expect(options.port.recover).toHaveBeenCalledTimes(2);
  });

  it.each(["succeeded", "failed", "cancelled", "interrupted"])("preserves already settled %s evidence", async (status) => {
    const options = ports();
    expect(await createExternalExecutionLifecycle(options).drain(run({ status }), "shutdown")).toBe(true);
    expect(options.persistIntent).not.toHaveBeenCalled();
    expect(options.port.drain).not.toHaveBeenCalled();
  });
});
