import { randomUUID } from "node:crypto";
import { and, eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  agentWakeupRequests, agents, companies, createDb, heartbeatRunEvents, heartbeatRuns,
  issueRecoveryActions, issues,
} from "@paperclipai/db";
import { getEmbeddedPostgresTestSupport, startEmbeddedPostgresTestDatabase } from "../../../__tests__/helpers/embedded-postgres.js";
import { createPostgresRunDispatchAdapter } from "./postgres.js";
import { LEGACY_RECOVERY_CAUSE, settleInterruptedNativeBootstrap, terminalizeLegacyExecution } from "../../../services/legacy-execution-recovery.js";

vi.mock("../../../services/agent-task-run-telemetry.js", () => ({ emitAgentTaskRun: vi.fn() }));

const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;
if (!support.supported) console.warn(`Skipping external execution writer PostgreSQL tests: ${support.reason ?? "unsupported environment"}`);

describePostgres("external execution run-dispatch and legacy writer PostgreSQL fences", () => {
  let db!: ReturnType<typeof createDb>;
  let temporary: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  const now = new Date("2026-10-04T02:00:00.000Z");

  beforeAll(async () => {
    temporary = await startEmbeddedPostgresTestDatabase("paperclip-external-execution-writers-");
    db = createDb(temporary.connectionString);
  }, 20_000);
  afterEach(() => { vi.restoreAllMocks(); });
  afterAll(async () => { await temporary?.cleanup(); });

  async function seed(input: {
    driverKind?: string | null; runStatus?: string; issueStatus?: string; context?: Record<string, unknown>;
  } = {}) {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const issueId = randomUUID();
    const runId = randomUUID();
    const wakeupRequestId = randomUUID();
    await db.insert(companies).values({
      id: companyId, name: "External execution writer regression", issuePrefix: `H${companyId.replaceAll("-", "").slice(0, 7).toUpperCase()}`,
      defaultResponsibleUserId: "external-execution-test-user", requireBoardApprovalForNewAgents: false,
    });
    // The agent has already changed back to a local adapter. Only the persisted
    // run driver can identify the old Hive execution.
    await db.insert(agents).values({
      id: agentId, companyId, name: "Changed local agent", role: "engineer", status: "active", adapterType: "codex_local",
      runtimeConfig: { heartbeat: { wakeOnDemand: true, maxConcurrentRuns: 1 } },
    });
    await db.insert(issues).values({ id: issueId, companyId, title: "Retain external execution", status: input.issueStatus ?? "in_progress", assigneeAgentId: agentId });
    await db.insert(agentWakeupRequests).values({ id: wakeupRequestId, companyId, agentId, source: "retry", status: "queued", runId });
    const [run] = await db.insert(heartbeatRuns).values({
      id: runId, companyId, agentId, wakeupRequestId, nativeIssueId: issueId, invocationSource: "retry",
      driverKind: input.driverKind === undefined ? "hive_runtime" : input.driverKind, runtimeMode: "legacy",
      status: input.runStatus ?? "scheduled_retry", scheduledRetryAt: now, scheduledRetryReason: "max_turns_continuation",
      errorCode: input.runStatus === "interrupted" ? "server_shutdown_interrupted" : null,
      runnerProfileJson: { adapterDispatch: { adapterType: "paperclip_runner" } },
      contextSnapshot: { issueId, ...input.context }, resultJson: { recordedWork: "retain" }, createdAt: now, updatedAt: now,
    }).returning();
    await db.update(issues).set({ executionRunId: runId, checkoutRunId: runId, executionAgentNameKey: "changedlocalagent", executionLockedAt: now })
      .where(and(eq(issues.companyId, companyId), eq(issues.id, issueId)));
    return { companyId, agentId, issueId, runId, wakeupRequestId, run };
  }

  type Fixture = Awaited<ReturnType<typeof seed>>;
  async function snapshot(fixture: Fixture) {
    const [run] = await db.select().from(heartbeatRuns).where(and(eq(heartbeatRuns.companyId, fixture.companyId), eq(heartbeatRuns.id, fixture.runId)));
    const [issue] = await db.select().from(issues).where(and(eq(issues.companyId, fixture.companyId), eq(issues.id, fixture.issueId)));
    const [wake] = await db.select().from(agentWakeupRequests).where(and(eq(agentWakeupRequests.companyId, fixture.companyId), eq(agentWakeupRequests.id, fixture.wakeupRequestId)));
    const events = await db.select().from(heartbeatRunEvents).where(and(eq(heartbeatRunEvents.companyId, fixture.companyId), eq(heartbeatRunEvents.runId, fixture.runId)));
    const actions = await db.select().from(issueRecoveryActions).where(and(eq(issueRecoveryActions.companyId, fixture.companyId), eq(issueRecoveryActions.sourceIssueId, fixture.issueId)));
    return { run, issue, wake, events, actions };
  }

  async function seedBootstrapHold(fixture: Fixture) {
    await db.insert(issueRecoveryActions).values({
      companyId: fixture.companyId, sourceIssueId: fixture.issueId, kind: "active_run_watchdog", ownerType: "board",
      cause: LEGACY_RECOVERY_CAUSE, fingerprint: `legacy-execution:${fixture.runId}`, nextAction: "Inspect recorded work",
      evidence: { runId: fixture.runId },
    });
  }

  /** Change the actual persisted row immediately before the real final UPDATE.
   * SQL and driver results are unchanged. This intentionally creates a stale
   * local decision inside the transaction to prove the independent SQL fence. */
  async function changeDriverBeforeFinalWrite(fixture: Fixture, operation: () => Promise<unknown>): Promise<unknown> {
    let changed = false;
    const transaction = db.transaction.bind(db);
    const transactionSpy = vi.spyOn(db, "transaction").mockImplementation((callback, config) => transaction(async (tx) => {
      const update = tx.update.bind(tx);
      vi.spyOn(tx, "update").mockImplementation((table) => {
        const builder = update(table);
        if (table !== heartbeatRuns) return builder;
        const set = builder.set.bind(builder);
        vi.spyOn(builder, "set").mockImplementation((values) => {
          const query = set(values);
          const execute = query.execute.bind(query);
          vi.spyOn(query, "execute").mockImplementation(async () => {
            if (!changed) {
              changed = true;
              await update(heartbeatRuns).set({ driverKind: "hive_runtime" })
                .where(and(eq(heartbeatRuns.id, fixture.runId), eq(heartbeatRuns.companyId, fixture.companyId)));
            }
            return execute();
          });
          return query;
        });
        return builder;
      });
      return callback(tx);
    }, config));
    try {
      const outcome = await operation();
      expect(changed).toBe(true);
      return outcome;
    } finally {
      transactionSpy.mockRestore();
    }
  }

  it.each(["in_progress", "done", "cancelled"])("retains a direct external due retry with an issue in %s", async (issueStatus) => {
    const fixture = await seed({ issueStatus });
    const before = await snapshot(fixture);
    const adapter = createPostgresRunDispatchAdapter(db);
    const due = await adapter.listDueRetries({ now, cutoff: now, limit: 50 });
    expect(due.some(row => row.runId === fixture.runId)).toBe(false);
    await expect(adapter.evaluateScheduledRetryGate({ ...fixture, now, retryReasonOverride: "max_turns_continuation" }))
      .resolves.toMatchObject({ allowed: false, errorCode: "external_execution_retained" });
    await expect(adapter.promoteOrCancelDueRetry({ ...fixture, now })).resolves.toEqual({ outcome: "external_execution_retained" });
    expect(await snapshot(fixture)).toEqual(before);
  });

  it.each(["queued", "running"] as const)("preserves a %s Hive run, issue locks and wake when bypassing the normal callers", async (runStatus) => {
    const fixture = await seed({ runStatus, issueStatus: "done", context: { wakeReason: "issue_commented", wakeCommentId: randomUUID() } });
    const before = await snapshot(fixture);
    const adapter = createPostgresRunDispatchAdapter(db);
    const dispatch = vi.fn(async () => "started");
    const input = { ...fixture, now, expectedStatus: runStatus };
    await expect(adapter.cancelStaleQueuedRun(input)).resolves.toEqual({ outcome: "external_execution_retained" });
    await expect(adapter.dispatchResolvedInteractionIfCurrent({ ...input, dispatch }))
      .resolves.toEqual({ dispatched: false, cancellation: { outcome: "external_execution_retained" } });
    expect(dispatch).not.toHaveBeenCalled();
    expect(await snapshot(fixture)).toEqual(before);
  });

  it("rereads the persisted driver after a real issue-lock wait", async () => {
    const fixture = await seed({ driverKind: null });
    const before = await snapshot(fixture);
    let acquired!: () => void;
    const locked = new Promise<void>((resolve) => { acquired = resolve; });
    const holder = db.transaction(async (tx) => {
      await tx.select({ id: issues.id }).from(issues)
        .where(and(eq(issues.companyId, fixture.companyId), eq(issues.id, fixture.issueId))).for("update");
      const [{ pid }] = await tx.execute<{ pid: number }>(sql`select pg_backend_pid() as pid`);
      acquired();
      let observedWait = false;
      const deadline = Date.now() + 3000;
      while (Date.now() < deadline) {
        const [{ waiting }] = await db.execute<{ waiting: boolean }>(sql`
          select exists (select 1 from pg_stat_activity where datname = current_database()
            and wait_event_type = 'Lock' and ${pid} = any(pg_blocking_pids(pid))) as waiting
        `);
        if (waiting) { observedWait = true; break; }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
      expect(observedWait).toBe(true);
      await tx.update(heartbeatRuns).set({ driverKind: "hive_runtime" })
        .where(and(eq(heartbeatRuns.companyId, fixture.companyId), eq(heartbeatRuns.id, fixture.runId)));
    });
    await Promise.race([locked, holder]);
    const [outcome] = await Promise.all([
      createPostgresRunDispatchAdapter(db).promoteOrCancelDueRetry({ ...fixture, now }), holder,
    ]);
    expect(outcome).toEqual({ outcome: "external_execution_retained" });
    expect(await snapshot(fixture)).toEqual({ ...before, run: { ...before.run, driverKind: "hive_runtime" } });
  });

  it("rejects legacy terminalization given a stale local input while the row is already Hive", async () => {
    const fixture = await seed({ runStatus: "failed", issueStatus: "done" });
    const before = await snapshot(fixture);
    await expect(terminalizeLegacyExecution({ db, run: { ...fixture.run, driverKind: null }, status: "failed", patch: { errorCode: "process_lost" } }))
      .resolves.toBeNull();
    expect(await snapshot(fixture)).toEqual(before);
  });

  it("retains the Hive bootstrap and reconciliation hold despite a stale paperclip_runner input", async () => {
    const fixture = await seed({ runStatus: "interrupted", issueStatus: "done" });
    await seedBootstrapHold(fixture);
    const before = await snapshot(fixture);
    await expect(settleInterruptedNativeBootstrap(db, { run: { ...fixture.run, driverKind: null }, providerDispatchStarted: false })).resolves.toBeNull();
    expect(await snapshot(fixture)).toEqual(before);
  });

  it.each([null, "codex_local"])("preserves promotion, cancellation, dispatch and profile writes for the %s local driver", async (driverKind) => {
    const adapter = createPostgresRunDispatchAdapter(db);
    const promoted = await seed({ driverKind });
    await expect(adapter.promoteOrCancelDueRetry({ ...promoted, now })).resolves.toMatchObject({ outcome: "promoted" });
    const promotion = await snapshot(promoted);
    expect(promotion.run.status).toBe("queued");
    expect(promotion.issue.executionRunId).toBe(promoted.runId);

    const suppressed = await seed({ driverKind, issueStatus: "done" });
    await expect(adapter.promoteOrCancelDueRetry({ ...suppressed, now })).resolves.toMatchObject({ outcome: "gate_suppressed" });
    const cancellation = await snapshot(suppressed);
    expect(cancellation.run.status).toBe("cancelled");
    expect(cancellation.issue.executionRunId).toBeNull();
    expect(cancellation.wake.status).toBe("cancelled");

    const queued = await seed({ driverKind, runStatus: "queued", issueStatus: "done" });
    await expect(adapter.cancelStaleQueuedRun({ ...queued, now, expectedStatus: "queued" })).resolves.toMatchObject({ outcome: "cancelled" });
    expect((await snapshot(queued)).issue.executionRunId).toBeNull();

    const profile = await seed({ driverKind, runStatus: "queued", context: { wakeReason: "issue_commented", wakeCommentId: randomUUID() } });
    await expect(adapter.cancelStaleQueuedRun({ ...profile, now, expectedStatus: "queued" })).resolves.toEqual({ outcome: "not_stale" });
    expect((await snapshot(profile)).run.runnerProfileJson).toMatchObject({ aiConnectionNonAssigneeCommentWake: false });

    const running = await seed({ driverKind, runStatus: "running" });
    const dispatch = vi.fn(async () => "started");
    const result = await adapter.dispatchResolvedInteractionIfCurrent({ ...running, now, expectedStatus: "running", dispatch });
    expect(result.dispatched).toBe(true);
    if (result.dispatched) await expect(result.resultPromise).resolves.toBe("started");
    expect(dispatch).toHaveBeenCalledTimes(1);
  });

  it.each([null, "codex_local"])("keeps legacy terminalization and bootstrap hold settlement for the %s local driver", async (driverKind) => {
    const terminal = await seed({ driverKind, runStatus: "running", issueStatus: "done" });
    await expect(terminalizeLegacyExecution({ db, run: terminal.run, status: "failed" })).resolves.toMatchObject({ status: "failed" });
    const settled = await snapshot(terminal);
    expect(settled.issue.executionRunId).toBeNull();
    expect(settled.issue.checkoutRunId).toBeNull();

    const bootstrap = await seed({ driverKind, runStatus: "interrupted", issueStatus: "done" });
    await seedBootstrapHold(bootstrap);
    await expect(settleInterruptedNativeBootstrap(db, { run: bootstrap.run, providerDispatchStarted: false })).resolves.toMatchObject({ status: "interrupted" });
    const reconciled = await snapshot(bootstrap);
    expect(reconciled.run.resultJson).toMatchObject({ executionRecovery: { kind: "bootstrap", providerWorkStarted: false } });
    expect(reconciled.actions).toEqual([expect.objectContaining({ status: "resolved", outcome: "false_positive" })]);
  });

  it.each(["promote", "suppress", "queued_cancel", "profile", "bootstrap"])("enforces the final %s SQL driver fence after a stale local decision", async (mode) => {
    const runStatus = mode === "bootstrap" ? "interrupted" : mode === "queued_cancel" || mode === "profile" ? "queued" : "scheduled_retry";
    const fixture = await seed({
      driverKind: null, runStatus, issueStatus: mode === "suppress" || mode === "queued_cancel" ? "done" : "in_progress",
      context: mode === "profile" ? { wakeReason: "issue_commented", wakeCommentId: randomUUID() } : undefined,
    });
    if (mode === "bootstrap") await seedBootstrapHold(fixture);
    const before = await snapshot(fixture);
    const adapter = createPostgresRunDispatchAdapter(db);
    const outcome = await changeDriverBeforeFinalWrite(fixture, () => mode === "bootstrap"
      ? settleInterruptedNativeBootstrap(db, { run: fixture.run, providerDispatchStarted: false })
      : mode === "queued_cancel" || mode === "profile"
        ? adapter.cancelStaleQueuedRun({ ...fixture, now, expectedStatus: "queued" })
        : adapter.promoteOrCancelDueRetry({ ...fixture, now }));
    if (mode === "bootstrap") expect(outcome).toBeNull();
    else expect(outcome).toMatchObject({ outcome: mode === "queued_cancel" ? "lost_race" : mode === "profile" ? "external_execution_retained" : "not_promoted" });
    expect(await snapshot(fixture)).toEqual({ ...before, run: { ...before.run, driverKind: "hive_runtime" } });
  });

  it("keeps the owned blocked issue projection if the final bootstrap fence rejects the write", async () => {
    const fixture = await seed({ driverKind: null, runStatus: "interrupted", issueStatus: "blocked" });
    const [issue] = await db.update(issues).set({ executionRunId: null, checkoutRunId: null })
      .where(and(eq(issues.companyId, fixture.companyId), eq(issues.id, fixture.issueId))).returning();
    await db.insert(issueRecoveryActions).values({
      companyId: fixture.companyId, sourceIssueId: fixture.issueId, kind: "active_run_watchdog", ownerType: "board",
      cause: LEGACY_RECOVERY_CAUSE, fingerprint: `legacy-execution:${fixture.runId}`, nextAction: "Inspect recorded work",
      evidence: { runId: fixture.runId, nativeBootstrapFailureBlock: { runId: fixture.runId, statusVersion: issue.statusVersion, previousStatus: "in_progress" } },
    });
    const before = await snapshot(fixture);
    await expect(changeDriverBeforeFinalWrite(fixture, () => settleInterruptedNativeBootstrap(db, { run: fixture.run, providerDispatchStarted: false })))
      .resolves.toBeNull();
    expect(await snapshot(fixture)).toEqual({ ...before, run: { ...before.run, driverKind: "hive_runtime" } });
  });

  it("keeps persisted writers inside the run company even with forged local input", async () => {
    const fixture = await seed({ driverKind: null, runStatus: "interrupted", issueStatus: "done" });
    const before = await snapshot(fixture);
    const otherCompanyId = randomUUID();
    const adapter = createPostgresRunDispatchAdapter(db);
    await expect(adapter.promoteOrCancelDueRetry({ ...fixture, companyId: otherCompanyId, now })).resolves.toEqual({ outcome: "not_promoted" });
    await expect(adapter.cancelStaleQueuedRun({ ...fixture, companyId: otherCompanyId, now, expectedStatus: "running" })).rejects.toMatchObject({ code: "run_not_found" });
    await expect(terminalizeLegacyExecution({ db, run: { ...fixture.run, companyId: otherCompanyId }, status: "failed" })).resolves.toBeNull();
    await expect(settleInterruptedNativeBootstrap(db, { run: { ...fixture.run, companyId: otherCompanyId }, providerDispatchStarted: false })).resolves.toBeNull();
    expect(await snapshot(fixture)).toEqual(before);
  });
});
