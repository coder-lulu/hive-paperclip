import { afterEach, describe, expect, it, vi } from "vitest";
import { drizzle } from "drizzle-orm/postgres-js";
import { agents, heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { createPostgresRunDispatchAdapter } from "./postgres.js";
import { appendHeartbeatRunEvent } from "../../../services/heartbeat-run-events.js";
import { emitAgentTaskRun } from "../../../services/agent-task-run-telemetry.js";

vi.mock("../../../middleware/logger.js", () => ({ logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() } }));
vi.mock("../../../services/execution-blocker.js", () => ({ getExecutionBlocker: vi.fn(async () => null) }));
vi.mock("../../../services/native-runtime/native-review-participant.js", () => ({ getNativeReviewAssignment: vi.fn(async () => null) }));
vi.mock("../../../services/agent-invokability.js", () => ({ evaluateAgentInvokabilityFromDb: vi.fn(async () => ({ invokable: true })) }));
vi.mock("../../../services/budgets.js", () => ({ budgetService: () => ({ getInvocationBlock: vi.fn(async () => null) }) }));
vi.mock("../../../services/chat-completion-delivery.js", () => ({ isCompletedOnboardingHandoffWake: vi.fn(async () => false) }));
vi.mock("../../../services/recovery/disposition-repair.js", () => ({ collectDispositionRepairSourceState: vi.fn() }));
vi.mock("../../../services/heartbeat-run-events.js", () => ({ appendHeartbeatRunEvent: vi.fn() }));
vi.mock("../../../services/agent-task-run-telemetry.js", () => ({ emitAgentTaskRun: vi.fn() }));
vi.mock("../../../services/issues.js", () => ({ issueService: () => ({ listDependencyReadiness: vi.fn(async () => new Map()) }) }));
vi.mock("../../../services/issue-tree-control.js", () => ({
  issueTreeControlService: () => ({ getActivePauseHoldGate: vi.fn(async () => null) }),
  isVerifiedIssueTreeControlInteractionWake: vi.fn(() => false),
  ISSUE_TREE_CONTROL_INTERACTION_WAKE_REASONS: new Set(["issue_commented"]),
}));
vi.mock("../../../services/issue-continuation-summary.js", () => ({
  continuationSummaryParksExecutor: vi.fn(() => false), getIssueContinuationSummaryDocument: vi.fn(),
}));

const runId = "00000000-0000-4000-8000-000000000011";
const companyId = "00000000-0000-4000-8000-000000000012";
const agentId = "00000000-0000-4000-8000-000000000013";
const issueId = "00000000-0000-4000-8000-000000000014";
const now = new Date("2026-10-04T01:00:00.000Z");

function makeRun(overrides: Record<string, unknown> = {}) {
  return {
    id: runId, companyId, agentId, driverKind: null, status: "scheduled_retry", runtimeMode: "legacy",
    contextSnapshot: { issueId }, scheduledRetryAt: now, scheduledRetryReason: "provider_failed",
    resultJson: null, wakeupRequestId: null, ...overrides,
  };
}

/** Actual Drizzle SQL compilation with fixed read results and empty write results.
 * This does not execute or simulate SQL; the PostgreSQL regressions prove fencing. */
function captureSql(run = makeRun(), issueStatus = "done", hasAgent = false) {
  const rawDb = drizzle.mock();
  const reads: Array<{ table: unknown; sql: string; params: unknown[] }> = [];
  const writes: Array<{ table: unknown; sql: string; params: unknown[] }> = [];
  const rows = new Map<unknown, unknown[]>([
    [heartbeatRuns, [run]],
    [issues, [{ id: issueId, companyId, status: issueStatus, assigneeAgentId: agentId, executionRunId: runId, checkoutRunId: runId }]],
    [agents, hasAgent ? [{ id: agentId, companyId, adapterType: "codex_local", status: "active" }] : []],
  ]);
  const select = rawDb.select.bind(rawDb);
  vi.spyOn(rawDb, "select").mockImplementation((fields) => {
    const builder = select(fields);
    const from = builder.from.bind(builder);
    vi.spyOn(builder, "from").mockImplementation((table) => {
      const query = from(table);
      vi.spyOn(query, "execute").mockImplementation(async () => {
        reads.push({ table, ...query.toSQL() });
        return rows.get(table) as never;
      });
      return query;
    });
    return builder;
  });
  const update = rawDb.update.bind(rawDb);
  vi.spyOn(rawDb, "update").mockImplementation((table) => {
    const builder = update(table);
    const set = builder.set.bind(builder);
    vi.spyOn(builder, "set").mockImplementation((values) => {
      const query = set(values);
      vi.spyOn(query, "execute").mockImplementation(async () => {
        writes.push({ table, ...query.toSQL() });
        return [] as never;
      });
      return query;
    });
    return builder;
  });
  vi.spyOn(rawDb, "transaction").mockImplementation(async (callback) => callback(rawDb as never));
  return { adapter: createPostgresRunDispatchAdapter(rawDb as unknown as Db), reads, writes };
}

function expectLocalFence(query: { sql: string; params: unknown[] }) {
  expect(query.sql).toMatch(/\("heartbeat_runs"\."driver_kind" is null or "heartbeat_runs"\."driver_kind" <> \$\d+\)/);
  expect(query.params).toContain("hive_runtime");
}

afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks(); });

describe("run-dispatch external execution fencing", () => {
  it.each(["done", "cancelled", "blocked", "in_progress"])("retains an external retry with a %s issue and a changed local adapter", async (issueStatus) => {
    const fixture = captureSql(makeRun({ driverKind: "hive_runtime" }), issueStatus, true);
    await expect(fixture.adapter.promoteOrCancelDueRetry({ runId, companyId, now }))
      .resolves.toEqual({ outcome: "external_execution_retained" });
    expect(fixture.writes).toEqual([]);
    expect(fixture.reads.some(({ table }) => table === agents)).toBe(false);
    expect(appendHeartbeatRunEvent).not.toHaveBeenCalled();
    expect(emitAgentTaskRun).not.toHaveBeenCalled();
    for (const query of fixture.reads) expect(query.params).toContain(companyId);
  });

  it.each(["queued", "running"] as const)("retains a direct %s cancellation and rejects direct dispatch", async (status) => {
    const fixture = captureSql(makeRun({ driverKind: "hive_runtime", status }));
    const input = { runId, companyId, expectedStatus: status, now };
    await expect(fixture.adapter.cancelStaleQueuedRun(input)).resolves.toEqual({ outcome: "external_execution_retained" });
    const dispatch = vi.fn(async () => "started");
    await expect(fixture.adapter.dispatchResolvedInteractionIfCurrent({ ...input, dispatch }))
      .resolves.toEqual({ dispatched: false, cancellation: { outcome: "external_execution_retained" } });
    expect(dispatch).not.toHaveBeenCalled();
    expect(fixture.writes).toEqual([]);
  });

  it("rejects read-only retry eligibility for the persisted external driver", async () => {
    const fixture = captureSql(makeRun({ driverKind: "hive_runtime" }), "in_progress", true);
    await expect(fixture.adapter.evaluateScheduledRetryGate({ runId, companyId, now, retryReasonOverride: "provider_failed" }))
      .resolves.toMatchObject({ allowed: false, errorCode: "external_execution_retained" });
    expect(fixture.reads).toHaveLength(1);
    expect(fixture.writes).toEqual([]);
  });

  it("compiles a NULL-compatible persisted-driver filter into the bounded due scan", async () => {
    const fixture = captureSql();
    await fixture.adapter.listDueRetries({ now, cutoff: now, limit: 50 });
    expect(fixture.reads).toHaveLength(1);
    expectLocalFence(fixture.reads[0]);
    expect(fixture.reads[0].params).toContain(50);
  });

  it.each(["promote", "suppress", "stale_cancel", "profile"])("compiles the company and driver fence into the final %s writer", async (mode) => {
    const profile = mode === "profile";
    const run = makeRun({
      status: mode === "stale_cancel" || profile ? "queued" : "scheduled_retry",
      contextSnapshot: profile ? { issueId, wakeReason: "issue_commented", wakeCommentId: "comment-1" } : { issueId },
    });
    const fixture = captureSql(run, mode === "promote" || profile ? "in_progress" : "done", mode === "promote");
    if (mode === "stale_cancel" || profile) {
      await fixture.adapter.cancelStaleQueuedRun({ runId, companyId, now, expectedStatus: "queued" });
    } else {
      await fixture.adapter.promoteOrCancelDueRetry({ runId, companyId, now });
    }
    expect(fixture.writes).toHaveLength(1);
    const query = fixture.writes[0];
    expect(query.table).toBe(heartbeatRuns);
    expect(query.sql).toMatch(/where .*"heartbeat_runs"\."company_id" = \$\d+/);
    expect(query.params).toContain(companyId);
    expectLocalFence(query);
    expect(appendHeartbeatRunEvent).not.toHaveBeenCalled();
    expect(emitAgentTaskRun).not.toHaveBeenCalled();
  });
});
