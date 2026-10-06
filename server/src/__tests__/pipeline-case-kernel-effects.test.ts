import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createDb, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS, getEmbeddedPostgresTestSupport,
  issueComments, issues, pipelineAutomationExecutions, pipelineCaseBlockers,
  pipelineCaseEvents, pipelineCaseIssueLinks, pipelineCases, pipelineStages,
  routineRuns, routines, startEmbeddedPostgresTestDatabase,
} from "@paperclipai/db";
import { transitionCaseInTransaction, reviewCaseInTransaction } from "../services/pipeline-case-kernel.js";
import { patchCaseContentInTransaction } from "../services/pipeline-case-content.js";
import { caseKernelFixture } from "./helpers/pipeline-case-kernel-fixture.js";

const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;
const actor = { type: "user", userId: "kernel-reviewer" } as const;

describePostgres("standalone original Case kernel effects", () => {
  let db: ReturnType<typeof createDb>;
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("paperclip-case-kernel-effects-");
    db = createDb(database.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);
  afterAll(async () => { await database?.cleanup(); });

  async function current(caseId: string) {
    const [row] = await db.select().from(pipelineCases).where(eq(pipelineCases.id, caseId));
    if (!row) throw new Error("Case vanished");
    return row;
  }
  async function child(fixture: Awaited<ReturnType<typeof caseKernelFixture>>, terminalKind: "done" | null = null) {
    const [row] = await db.insert(pipelineCases).values({ companyId: fixture.company.id,
      pipelineId: fixture.pipeline.id, stageId: fixture.case.stageId, caseKey: randomUUID(),
      title: "Child", parentCaseId: fixture.case.id, terminalKind }).returning();
    if (!row) throw new Error("Missing child");
    return row;
  }
  async function automate(fixture: Awaited<ReturnType<typeof caseKernelFixture>>, key: string) {
    const stage = fixture.stages.get(key);
    if (!stage) throw new Error("No automation stage");
    const [routine] = await db.insert(routines).values({ companyId: fixture.company.id, title: "Descriptor only" }).returning();
    if (!routine) throw new Error("No routine fixture");
    await db.update(pipelineStages).set({ config: {
      ...stage.config, onEnter: { type: "run_routine", routineId: routine.id },
    } }).where(eq(pipelineStages.id, stage.id));
    return routine;
  }
  function input(fixture: Awaited<ReturnType<typeof caseKernelFixture>>) {
    return { companyId: fixture.company.id, caseId: fixture.case.id,
      expectedVersion: 1, toStageKey: "produce", actor };
  }

  it("returns no routine effects when onEnter is absent", async () => {
    const fixture = await caseKernelFixture(db);
    const result = await db.transaction((tx) => transitionCaseInTransaction(tx, input(fixture)));
    expect(result.effects.automationLedgers).toEqual([]);
    expect(result.case.version).toBe(2);
    expect(result.authoritativeCase).toEqual(await current(fixture.case.id));
  });
  it("returns the original pending ledger without executing a routine", async () => {
    const fixture = await caseKernelFixture(db);
    const routine = await automate(fixture, "produce");
    const result = await db.transaction((tx) => transitionCaseInTransaction(tx, input(fixture)));
    expect(result.effects.automationLedgers).toHaveLength(1);
    expect(result.automationLedger).toMatchObject({ routineId: routine.id,
      status: "failed", error: "pending_dispatch", executionIssueId: null });
    expect(await db.select().from(routineRuns).where(eq(routineRuns.companyId, fixture.company.id))).toHaveLength(0);
    expect(await db.select().from(issues).where(eq(issues.companyId, fixture.company.id))).toHaveLength(0);
  });
  it("lets an unsupported effect reject roll back Case, event and ledger", async () => {
    const fixture = await caseKernelFixture(db);
    await automate(fixture, "produce");
    await expect(db.transaction(async (tx) => {
      const result = await transitionCaseInTransaction(tx, input(fixture));
      expect(result.effects.automationLedgers).toHaveLength(1);
      throw new Error("unsupported routine effect");
    })).rejects.toThrow("unsupported routine effect");
    expect((await current(fixture.case.id)).version).toBe(1);
    expect(await db.select().from(pipelineCaseEvents).where(eq(pipelineCaseEvents.caseId, fixture.case.id))).toHaveLength(0);
    expect(await db.select().from(pipelineAutomationExecutions).where(eq(pipelineAutomationExecutions.caseId, fixture.case.id))).toHaveLength(0);
  });
  it("preserves the stale first row while rereading recursive entry advancement", async () => {
    const fixture = await caseKernelFixture(db, { configs: { produce: { autoAdvanceOnChildrenTerminal: "done" } },
      casePatch: { childCount: 1, terminalChildCount: 1 } });
    await child(fixture, "done");
    const result = await db.transaction((tx) => transitionCaseInTransaction(tx, input(fixture)));
    expect(result.case).toMatchObject({ version: 2, stageId: fixture.stages.get("produce")?.id });
    expect(result.authoritativeCase).toMatchObject({ version: 3, terminalKind: "done" });
  });
  it("retains zero-child entry semantics and visited-stage cycle protection", async () => {
    const empty = await caseKernelFixture(db, { configs: { produce: { autoAdvanceOnChildrenTerminal: "done" } } });
    const emptyResult = await db.transaction((tx) => transitionCaseInTransaction(tx, input(empty)));
    expect(emptyResult.authoritativeCase.terminalKind).toBeNull();
    const cycle = await caseKernelFixture(db, { configs: {
      draft: { autoAdvanceOnChildrenTerminal: "produce" }, produce: { autoAdvanceOnChildrenTerminal: "draft" },
    }, casePatch: { childCount: 1, terminalChildCount: 1 } });
    await child(cycle, "done");
    const result = await db.transaction((tx) => transitionCaseInTransaction(tx, input(cycle)));
    expect(result.case.version).toBe(2);
    expect(result.authoritativeCase).toMatchObject({ version: 4, stageId: cycle.stages.get("produce")?.id });
  });
  it("retains parent rollups, DB comments and every recursively collected effect", async () => {
    const fixture = await caseKernelFixture(db, { configs: { draft: { autoAdvanceOnChildrenTerminal: "done" } },
      casePatch: { childCount: 1 } });
    await automate(fixture, "done");
    const childRow = await child(fixture);
    const [issue] = await db.insert(issues).values({ companyId: fixture.company.id,
      title: "Origin", status: "todo", priority: "medium" }).returning();
    if (!issue) throw new Error("No origin Issue");
    await db.insert(pipelineCaseIssueLinks).values({ companyId: fixture.company.id,
      caseId: fixture.case.id, issueId: issue.id, role: "origin" });
    const result = await db.transaction((tx) => transitionCaseInTransaction(tx, {
      ...input(fixture), caseId: childRow.id, toStageKey: "done",
    }));
    expect(result.effects.automationLedgers).toHaveLength(2);
    expect((await current(fixture.case.id))).toMatchObject({ terminalKind: "done", terminalChildCount: 1 });
    expect(await db.select().from(issueComments).where(eq(issueComments.issueId, issue.id))).toHaveLength(1);
  });
  it("keeps child completion when a parent auto-advance gate fails", async () => {
    const fixture = await caseKernelFixture(db, { configs: { draft: { autoAdvanceOnChildrenTerminal: "done" } },
      casePatch: { childCount: 1 } });
    const childRow = await child(fixture);
    const [blocker] = await db.insert(pipelineCases).values({ companyId: fixture.company.id,
      pipelineId: fixture.pipeline.id, stageId: fixture.case.stageId, caseKey: randomUUID(), title: "Open blocker" }).returning();
    if (!blocker) throw new Error("No blocker");
    await db.insert(pipelineCaseBlockers).values({ companyId: fixture.company.id,
      caseId: fixture.case.id, blockedByCaseId: blocker.id });
    await db.transaction((tx) => transitionCaseInTransaction(tx, { ...input(fixture), caseId: childRow.id, toStageKey: "done" }));
    expect((await current(childRow.id)).terminalKind).toBe("done");
    expect((await current(fixture.case.id))).toMatchObject({ terminalKind: null, terminalChildCount: 1 });
  });
  it("rolls back review edits and drift comments with a failed transition", async () => {
    const fixture = await caseKernelFixture(db, { stageKey: "review", configs: { review: { approveToStageKey: "missing" } } });
    const [dependent] = await db.insert(pipelineCases).values({ companyId: fixture.company.id,
      pipelineId: fixture.pipeline.id, stageId: fixture.case.stageId, caseKey: randomUUID(), title: "Dependent" }).returning();
    const [issue] = await db.insert(issues).values({ companyId: fixture.company.id, title: "Work", status: "todo", priority: "medium" }).returning();
    if (!dependent || !issue) throw new Error("No drift fixture");
    await db.insert(pipelineCaseBlockers).values({ companyId: fixture.company.id, caseId: dependent.id, blockedByCaseId: fixture.case.id });
    await db.insert(pipelineCaseIssueLinks).values({ companyId: fixture.company.id, caseId: dependent.id, issueId: issue.id, role: "work" });
    await expect(db.transaction((tx) => reviewCaseInTransaction(tx, {
      companyId: fixture.company.id, caseId: fixture.case.id, expectedVersion: 1,
      actor, decision: "approve", edits: { title: "Uncommitted change" },
    }))).rejects.toMatchObject({ status: 404 });
    expect((await current(fixture.case.id))).toMatchObject({ title: "Case fixture", version: 1 });
    expect(await db.select().from(issueComments).where(eq(issueComments.issueId, issue.id))).toHaveLength(0);
    expect(await db.select().from(pipelineCaseEvents).where(eq(pipelineCaseEvents.caseId, dependent.id))).toHaveLength(0);
  });
  it("returns review effects and its authoritative Case on the same transaction", async () => {
    const fixture = await caseKernelFixture(db, { stageKey: "review", configs: { review: { approveToStageKey: "produce" } } });
    await automate(fixture, "produce");
    const result = await db.transaction((tx) => reviewCaseInTransaction(tx, {
      companyId: fixture.company.id, caseId: fixture.case.id, expectedVersion: 1,
      actor, decision: "approve", edits: { title: "Reviewed" },
    }));
    expect(result.effects.automationLedgers).toHaveLength(1);
    expect(result.authoritativeCase).toMatchObject({ title: "Reviewed", version: 3 });
    expect(result.reviewEvent.payload.approvedTransitionVersion).toBe(3);
  });
  it("preserves terminal reparent pending ledgers outside the original accumulator", async () => {
    const parent = await caseKernelFixture(db, { configs: { draft: { autoAdvanceOnChildrenTerminal: "done" } } });
    await automate(parent, "done");
    const done = parent.stages.get("done");
    if (!done) throw new Error("No terminal stage");
    const [terminal] = await db.insert(pipelineCases).values({ companyId: parent.company.id,
      pipelineId: parent.pipeline.id, stageId: done.id,
      caseKey: randomUUID(), title: "Terminal child", terminalKind: "done" }).returning();
    if (!terminal) throw new Error("No terminal child");
    const result = await db.transaction((tx) => patchCaseContentInTransaction(tx, {
      companyId: parent.company.id, caseId: terminal.id, expectedVersion: 1,
      parentCaseId: parent.case.id, actor,
    }));
    expect(result.case.version).toBe(1);
    expect(result).not.toHaveProperty("automationLedger");
    expect((await current(parent.case.id)).terminalKind).toBe("done");
    expect(await db.select().from(pipelineAutomationExecutions).where(eq(pipelineAutomationExecutions.caseId, parent.case.id)))
      .toEqual([expect.objectContaining({ status: "failed", error: "pending_dispatch" })]);
  });
});
