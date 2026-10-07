import { afterEach, describe, expect, it, vi } from "vitest";
import { drizzle } from "drizzle-orm/postgres-js";
import { heartbeatRuns, issues, type Db } from "@paperclipai/db";
import { legacyExecutionNeedsReconciliation, settleInterruptedNativeBootstrap, terminalizeLegacyExecution } from "./legacy-execution-recovery.js";

vi.mock("./issue-recovery-actions.js", () => ({ issueRecoveryActionService: () => ({}) }));
vi.mock("./agent-conversations.js", () => ({ isSupersededConversationRun: vi.fn(() => false) }));
vi.mock("./issues.js", () => ({ issueService: () => ({ update: vi.fn() }) }));
vi.mock("./activity-log.js", () => ({ logActivity: vi.fn() }));

const runId = "00000000-0000-4000-8000-000000000021";
const companyId = "00000000-0000-4000-8000-000000000022";
const issueId = "00000000-0000-4000-8000-000000000023";

function makeRun(overrides: Partial<typeof heartbeatRuns.$inferSelect> = {}): typeof heartbeatRuns.$inferSelect {
  return {
    id: runId, companyId, driverKind: null, runtimeMode: "legacy", status: "interrupted", nativeIssueId: issueId,
    contextSnapshot: { issueId }, runtimeModeResolvedAt: null, errorCode: "server_shutdown_interrupted",
    processPid: null, processGroupId: null, resultJson: {}, scheduledRetryAttempt: 0,
    runnerProfileJson: { adapterDispatch: { adapterType: "paperclip_runner" } }, ...overrides,
  } as typeof heartbeatRuns.$inferSelect;
}

/** Fixed reads, actual Drizzle query compilation, no database execution. */
function captureSql(persisted = makeRun()) {
  const rawDb = drizzle.mock();
  const writes: Array<{ table: unknown; sql: string; params: unknown[] }> = [];
  const select = rawDb.select.bind(rawDb);
  vi.spyOn(rawDb, "select").mockImplementation((fields) => {
    const builder = select(fields);
    const from = builder.from.bind(builder);
    vi.spyOn(builder, "from").mockImplementation((table) => {
      const query = from(table);
      vi.spyOn(query, "execute").mockResolvedValue((table === heartbeatRuns ? [persisted]
        : table === issues ? [{ id: issueId, companyId, status: "done", executionRunId: runId, checkoutRunId: runId }] : []) as never);
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
      vi.spyOn(query, "execute").mockImplementation(async () => { writes.push({ table, ...query.toSQL() }); return [] as never; });
      return query;
    });
    return builder;
  });
  const transaction = vi.spyOn(rawDb, "transaction").mockImplementation(async (callback) => callback(rawDb as never));
  vi.spyOn(rawDb, "execute").mockResolvedValue([] as never);
  return { db: rawDb as unknown as Db, transaction, writes };
}

afterEach(() => { vi.restoreAllMocks(); vi.clearAllMocks(); });

describe("legacy recovery external execution fencing", () => {
  it.each(["failed", "timed_out", "interrupted", "cancelled"])("keeps %s Hive runs outside local reconciliation despite unsafe archive or runner metadata", (status) => {
    expect(legacyExecutionNeedsReconciliation(makeRun({ driverKind: "hive_runtime", status, resultJson: { workspaceRestoreFailure: "restore_unsafe_archive" } })))
      .toBe(false);
  });

  it.each([null, "codex_local"])("keeps reconciliation behavior for the %s local driver", (driverKind) => {
    expect(legacyExecutionNeedsReconciliation(makeRun({ driverKind, status: "failed", errorCode: "process_lost" }))).toBe(true);
  });

  it("rejects direct Hive terminalization and bootstrap settlement before any transaction", async () => {
    const run = makeRun({ driverKind: "hive_runtime" });
    const fixture = captureSql(run);
    await expect(terminalizeLegacyExecution({ db: fixture.db, run, status: "failed" })).resolves.toBeNull();
    await expect(settleInterruptedNativeBootstrap(fixture.db, { run, providerDispatchStarted: false })).resolves.toBeNull();
    expect(fixture.transaction).not.toHaveBeenCalled();
    expect(fixture.writes).toEqual([]);
  });

  it("rereads the persisted bootstrap driver before settling a stale local input", async () => {
    const fixture = captureSql(makeRun({ driverKind: "hive_runtime" }));
    await expect(settleInterruptedNativeBootstrap(fixture.db, { run: makeRun(), providerDispatchStarted: false })).resolves.toBeNull();
    expect(fixture.writes).toEqual([]);
  });

  it.each(["terminalize", "bootstrap"])("compiles a company-scoped NULL-compatible driver fence in the final %s writer", async (mode) => {
    const fixture = captureSql();
    if (mode === "terminalize") {
      await expect(terminalizeLegacyExecution({ db: fixture.db, run: makeRun(), status: "failed" })).resolves.toBeNull();
    } else {
      await expect(settleInterruptedNativeBootstrap(fixture.db, { run: makeRun(), providerDispatchStarted: false })).resolves.toBeNull();
    }
    expect(fixture.writes).toHaveLength(1);
    const query = fixture.writes[0];
    expect(query.table).toBe(heartbeatRuns);
    expect(query.sql).toMatch(/where .*"heartbeat_runs"\."company_id" = \$\d+/);
    expect(query.sql).toMatch(/\("heartbeat_runs"\."driver_kind" is null or "heartbeat_runs"\."driver_kind" <> \$\d+\)/);
    expect(query.params).toContain(companyId);
    expect(query.params).toContain("hive_runtime");
  });
});
