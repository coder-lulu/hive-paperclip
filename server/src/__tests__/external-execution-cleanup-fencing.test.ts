import { afterEach, describe, expect, it, vi } from "vitest";
import { drizzle } from "drizzle-orm/postgres-js";
import { heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { createPostgresWatchdogAdapter } from "../modules/active-run-watchdog/adapters/postgres.js";
import type { FoldSourceResolvedRunInput } from "../modules/active-run-watchdog/application/types.js";
import { isPidAlive, isProcessGroupAlive } from "../services/local-service-supervisor.js";
import { recoveryService } from "../services/recovery/service.js";

vi.mock("../middleware/logger.js", () => ({
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() },
}));
vi.mock("../adapters/index.js", () => ({ runningProcesses: new Map() }));
vi.mock("../services/native-runtime/chat-attachment-reuse.js", () => ({ authorizeChatConversationForBoundRun: vi.fn() }));

vi.mock("../services/local-service-supervisor.js", () => ({
  isPidAlive: vi.fn(() => false),
  isProcessGroupAlive: vi.fn(() => false),
  terminateLocalService: vi.fn(),
}));
vi.mock("../services/agent-task-run-telemetry.js", () => ({ emitAgentTaskRun: vi.fn() }));
// These services are constructed by the real cleanup entry points but their
// unrelated issue/recovery workflows are never called by these guarded writes.
vi.mock("../services/issues.js", () => ({
  TERMINAL_HEARTBEAT_RUN_STATUSES: new Set(["succeeded", "failed", "cancelled", "timed_out", "interrupted"]),
  issueService: () => ({}),
  executeIssuePostCommitActions: vi.fn(),
}));
vi.mock("../services/issue-recovery-actions.js", () => ({ issueRecoveryActionService: () => ({}) }));
vi.mock("../services/issue-tree-control.js", () => ({ issueTreeControlService: () => ({}) }));
vi.mock("../services/budgets.js", () => ({ budgetService: () => ({}) }));
vi.mock("../services/slack-conversation-lifecycle.js", () => ({ settleSlackConversation: vi.fn() }));
vi.mock("../services/agent-conversations.js", () => ({
  isConversation: vi.fn(), isWaitingConversation: vi.fn(), isSupersededConversationRun: vi.fn(),
  settleConversationTurn: vi.fn(), deliverConversationComments: vi.fn(),
}));
vi.mock("../config.js", () => ({
  loadConfig: () => { throw new Error("Cleanup SQL tests must not read instance configuration"); },
}));
vi.mock("../storage/index.js", () => ({
  createStorageServiceFromConfig: () => { throw new Error("Cleanup SQL tests must not access storage"); },
  getStorageService: () => { throw new Error("Cleanup SQL tests must not access storage"); },
}));

const runId = "00000000-0000-4000-8000-000000000001";
const companyId = "00000000-0000-4000-8000-000000000002";
const agentId = "00000000-0000-4000-8000-000000000003";
const issueId = "00000000-0000-4000-8000-000000000004";
const now = new Date("2026-10-04T00:00:00.000Z");

function makeRun(overrides: Record<string, unknown> = {}) {
  return {
    id: runId, companyId, agentId, driverKind: null, status: "running", runtimeMode: "legacy",
    controllerBootId: null, nativePhase: null, processPid: process.pid, processGroupId: process.pid,
    contextSnapshot: { issueId }, resultJson: null, wakeupRequestId: null,
    lastOutputAt: null, lastOutputSeq: 0, lastOutputStream: null,
    processStartedAt: now, startedAt: now, createdAt: now, finishedAt: null,
    error: null, errorCode: null, ...overrides,
  };
}

function makeIssue() {
  return { id: issueId, companyId, status: "done", checkoutRunId: runId, executionRunId: runId };
}

/** Capture actual Drizzle SQL without a connection. This fixture does not execute
 * SQL or simulate database fencing; the PostgreSQL suites test that behavior. */
function createSqlCapture(run = makeRun(), issue = makeIssue()) {
  const rawDb = drizzle.mock();
  const compiledUpdates: Array<{ table: unknown; sql: string; params: unknown[] }> = [];
  const rows = new Map<unknown, unknown[]>([[heartbeatRuns, [run]], [issues, [issue]]]);
  const originalSelect = rawDb.select.bind(rawDb);
  vi.spyOn(rawDb, "select").mockImplementation((fields) => {
    const builder = originalSelect(fields);
    const originalFrom = builder.from.bind(builder);
    vi.spyOn(builder, "from").mockImplementation((table) => {
      const query = originalFrom(table);
      vi.spyOn(query, "execute").mockResolvedValue(rows.get(table) as never);
      return query;
    });
    return builder;
  });
  const originalUpdate = rawDb.update.bind(rawDb);
  vi.spyOn(rawDb, "update").mockImplementation((table) => {
    const builder = originalUpdate(table);
    const originalSet = builder.set.bind(builder);
    vi.spyOn(builder, "set").mockImplementation((values) => {
      const query = originalSet(values);
      vi.spyOn(query, "execute").mockImplementation(async () => {
        compiledUpdates.push({ table, ...query.toSQL() });
        return [];
      });
      return query;
    });
    return builder;
  });
  const transaction = vi.spyOn(rawDb, "transaction").mockImplementation(async (callback) => callback(rawDb as never));
  return { db: rawDb as unknown as Db, compiledUpdates, transaction };
}

function expectLocalRunWriteFence(query: { sql: string; params: unknown[] }) {
  expect(query.sql).toMatch(/where .*"heartbeat_runs"\."company_id" = \$\d+/);
  expect(query.sql).toMatch(/\("heartbeat_runs"\."driver_kind" is null or "heartbeat_runs"\."driver_kind" <> \$\d+\)/);
  expect(query.params).toContain(companyId);
  expect(query.params).toContain("hive_runtime");
}

afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks(); });

describe("external execution cleanup fences without a database process", () => {
  it.each(["running", "succeeded"])("retains %s external runs and locks before any PID probe or writer", async (status) => {
    const fixture = createSqlCapture(makeRun({ driverKind: "hive_runtime", status }));

    await expect(recoveryService(fixture.db, { enqueueWakeup: vi.fn() }).sweepStaleIssueLocks())
      .resolves.toEqual({ cleared: 0, issueIds: [], terminalizedRunIds: [] });
    expect(isPidAlive).not.toHaveBeenCalled();
    expect(isProcessGroupAlive).not.toHaveBeenCalled();
    expect(fixture.compiledUpdates).toEqual([]);
  });

  it("compiles an independent persisted-driver fence into the stale-run final UPDATE", async () => {
    const fixture = createSqlCapture();
    await recoveryService(fixture.db, { enqueueWakeup: vi.fn() }).sweepStaleIssueLocks();

    expect(fixture.compiledUpdates).toHaveLength(1);
    expect(fixture.compiledUpdates[0].table).toBe(heartbeatRuns);
    expectLocalRunWriteFence(fixture.compiledUpdates[0]);
  });

  it("compiles a company-scoped external-driver check on both current pointers in lock cleanup", async () => {
    const fixture = createSqlCapture(makeRun({ status: "failed" }));
    await recoveryService(fixture.db, { enqueueWakeup: vi.fn() }).sweepStaleIssueLocks();

    expect(fixture.compiledUpdates).toHaveLength(1);
    const query = fixture.compiledUpdates[0];
    expect(query.table).toBe(issues);
    expect(query.sql).toContain('not exists (select');
    expect(query.sql).toContain('"heartbeat_runs"."company_id" = "issues"."company_id"');
    expect(query.sql).toContain('"heartbeat_runs"."id" = "issues"."checkout_run_id"');
    expect(query.sql).toContain('"heartbeat_runs"."id" = "issues"."execution_run_id"');
    expect(query.sql).toMatch(/"heartbeat_runs"\."driver_kind" = \$\d+/);
    expect(query.params).toContain("hive_runtime");
    expect(query.params).toContain(companyId);
  });

  function makeFoldInput(driverKind: string | null): FoldSourceResolvedRunInput {
    return {
      run: {
        ...makeRun(), driverKind, sourceIssueId: issueId,
      } as FoldSourceResolvedRunInput["run"],
      sourceIssue: { id: issueId, identifier: "HIVE-1", status: "done", originKind: "manual", isRecoveryOriginKind: false },
      evidence: { kind: "activity", id: "same-run-activity", createdAt: now, action: "issue.updated" },
      existingEvaluation: null, silenceStartedAt: now, silenceAgeMs: 0, now,
      cleanup: { attempted: false, outcome: "no_process_metadata", adapterType: "codex_local" },
    };
  }

  it("passes the persisted driver from database rows into watchdog snapshots", async () => {
    const fixture = createSqlCapture(makeRun({ driverKind: "hive_runtime" }));
    const adapter = createPostgresWatchdogAdapter(fixture.db);

    await expect(adapter.findRunForCompany(companyId, runId)).resolves.toMatchObject({ driverKind: "hive_runtime" });
    await expect(adapter.findCandidateSilentRuns({ companyId, suspicionBefore: now }))
      .resolves.toEqual([expect.objectContaining({ driverKind: "hive_runtime" })]);
  });

  it("rejects an external direct writer call before opening a transaction", async () => {
    const fixture = createSqlCapture();
    await expect(createPostgresWatchdogAdapter(fixture.db).foldSourceResolvedRun(companyId, makeFoldInput("hive_runtime")))
      .resolves.toEqual({ kind: "stale" });
    expect(fixture.transaction).not.toHaveBeenCalled();
    expect(fixture.compiledUpdates).toEqual([]);
  });

  it("compiles the persisted driver fence for a direct writer with a local snapshot", async () => {
    const fixture = createSqlCapture(makeRun({ driverKind: "hive_runtime" }));
    await expect(createPostgresWatchdogAdapter(fixture.db).foldSourceResolvedRun(companyId, makeFoldInput(null)))
      .resolves.toEqual({ kind: "stale" });
    expect(fixture.compiledUpdates).toHaveLength(1);
    expectLocalRunWriteFence(fixture.compiledUpdates[0]);
  });
});
