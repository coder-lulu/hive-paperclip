import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  createDb, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS, getEmbeddedPostgresTestSupport,
  pipelineCaseBlockers, pipelineCaseEvents, pipelineCases, pipelineTransitions,
  startEmbeddedPostgresTestDatabase,
} from "@paperclipai/db";
import { pipelineService, type PipelineActor } from "../services/pipelines.js";
import { caseKernelFixture } from "./helpers/pipeline-case-kernel-fixture.js";

const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;
const actor: PipelineActor = { type: "user", userId: "kernel-reviewer" };

describePostgres("original Case transaction graph regressions", () => {
  let db: ReturnType<typeof createDb>;
  let svc: ReturnType<typeof pipelineService>;
  let database: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  beforeAll(async () => {
    database = await startEmbeddedPostgresTestDatabase("paperclip-case-kernel-");
    db = createDb(database.connectionString);
    svc = pipelineService(db, { heartbeat: { wakeup: async () => null } });
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);
  afterAll(async () => { await database?.cleanup(); });

  async function current(caseId: string) {
    const [row] = await db.select().from(pipelineCases).where(eq(pipelineCases.id, caseId));
    if (!row) throw new Error("Case vanished");
    return row;
  }
  async function events(caseId: string) {
    return db.select().from(pipelineCaseEvents).where(eq(pipelineCaseEvents.caseId, caseId));
  }
  function transition(fixture: Awaited<ReturnType<typeof caseKernelFixture>>, extra = {}) {
    return svc.transitionCase({ companyId: fixture.company.id, caseId: fixture.case.id,
      expectedVersion: fixture.case.version, toStageKey: "produce", actor, ...extra });
  }

  it("keeps scoped access and stale CAS failures free of events", async () => {
    const fixture = await caseKernelFixture(db);
    await expect(transition(fixture, { companyId: randomUUID() })).rejects.toMatchObject({ status: 404 });
    await expect(transition(fixture, { expectedVersion: 9 })).rejects.toMatchObject({ details: { code: "version_conflict" } });
    expect(await events(fixture.case.id)).toHaveLength(0);
    expect((await current(fixture.case.id)).version).toBe(1);
  });
  it("lets only one simultaneous transition consume a version", async () => {
    const fixture = await caseKernelFixture(db);
    const results = await Promise.allSettled([transition(fixture), transition(fixture)]);
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
    expect((await current(fixture.case.id)).version).toBe(2);
    expect(await events(fixture.case.id)).toHaveLength(1);
  });
  it("protects a lease and retains matching token authority", async () => {
    const token = randomUUID();
    const fixture = await caseKernelFixture(db, { casePatch: { leaseOwnerType: "user",
      leaseUserId: "other", leaseToken: token, leaseExpiresAt: new Date(Date.now() + 60000) } });
    await expect(transition(fixture)).rejects.toMatchObject({ details: { code: "lease_held" } });
    await expect(transition(fixture, { leaseToken: token })).resolves.toMatchObject({ case: { version: 2 } });
  });
  it("expires a lease and clears it on terminal entry", async () => {
    const fixture = await caseKernelFixture(db, { casePatch: { leaseOwnerType: "user",
      leaseUserId: "other", leaseToken: randomUUID(), leaseExpiresAt: new Date(0) } });
    await transition(fixture, { toStageKey: "done" });
    expect((await current(fixture.case.id)).leaseToken).toBeNull();
    expect((await events(fixture.case.id)).map((event) => event.type)).toEqual(["lease_expired", "transitioned"]);
  });
  it("rolls back the mutation when agent run provenance is absent", async () => {
    const fixture = await caseKernelFixture(db);
    await expect(transition(fixture, { actor: { type: "agent", agentId: randomUUID(), runId: "" } }))
      .rejects.toMatchObject({ details: { code: "run_id_required" } });
    expect((await current(fixture.case.id)).version).toBe(1);
    expect(await events(fixture.case.id)).toHaveLength(0);
  });
  it("retains human approval and disabled stage guards", async () => {
    const review = await caseKernelFixture(db, { stageKey: "review" });
    await expect(transition(review, { actor: { type: "system" }, toStageKey: "done" }))
      .rejects.toMatchObject({ status: 403, details: { code: "review_required" } });
    const disabled = await caseKernelFixture(db, { configs: { produce: { disabled: true } } });
    await expect(transition(disabled)).rejects.toMatchObject({ details: { code: "stage_disabled" } });
  });
  it("retains edge force reason and transition_forced events", async () => {
    const fixture = await caseKernelFixture(db, { enforceTransitions: true });
    await expect(transition(fixture, { force: true, reason: " " }))
      .rejects.toMatchObject({ details: { code: "transition_not_allowed" } });
    await transition(fixture, { force: true, reason: "  approved override  " });
    expect((await events(fixture.case.id)).map((event) => event.type)).toEqual(["transitioned", "transition_forced"]);
    const ordinary = await caseKernelFixture(db, { enforceTransitions: true });
    const destination = ordinary.stages.get("produce");
    if (!destination) throw new Error("No destination");
    await db.insert(pipelineTransitions).values({ pipelineId: ordinary.pipeline.id,
      fromStageId: ordinary.case.stageId, toStageId: destination.id });
    await expect(transition(ordinary)).resolves.toMatchObject({ case: { version: 2 } });
  });
  it.each([null, "cancelled", "done"])("preserves blocker disposition %s", async (terminalKind) => {
    const fixture = await caseKernelFixture(db);
    const [blocker] = await db.insert(pipelineCases).values({ companyId: fixture.company.id,
      pipelineId: fixture.pipeline.id, stageId: fixture.case.stageId, caseKey: randomUUID(), title: "Blocker", terminalKind }).returning();
    if (!blocker) throw new Error("No blocker");
    await db.insert(pipelineCaseBlockers).values({ companyId: fixture.company.id,
      caseId: fixture.case.id, blockedByCaseId: blocker.id });
    if (terminalKind === "done") await expect(transition(fixture)).resolves.toMatchObject({ case: { version: 2 } });
    else await expect(transition(fixture)).rejects.toMatchObject({ details: { code: "blocked" } });
  });
  it("retains expected-child and unfinished-child gates", async () => {
    const expected = await caseKernelFixture(db, { configs: { draft: { requireChildrenTerminal: true } },
      casePatch: { fields: { expectedChildren: 1 } } });
    await expect(transition(expected)).rejects.toMatchObject({ details: { code: "expected_children_mismatch" } });
    const unfinished = await caseKernelFixture(db, { configs: { draft: { requireChildrenTerminal: true } },
      casePatch: { childCount: 1 } });
    await expect(transition(unfinished)).rejects.toMatchObject({ details: { code: "children_not_terminal" } });
  });
  it("requires acknowledgement for upstream drift", async () => {
    const fixture = await caseKernelFixture(db, { configs: { draft: { requireNoUnresolvedDrift: true } } });
    await db.insert(pipelineCaseEvents).values({ companyId: fixture.company.id, caseId: fixture.case.id,
      type: "upstream_drift", actorType: "system", payload: { upstreamCaseKey: "upstream" }, createdAt: new Date(1000) });
    await expect(transition(fixture)).rejects.toMatchObject({ details: { code: "unresolved_drift" } });
    await db.insert(pipelineCaseEvents).values({ companyId: fixture.company.id, caseId: fixture.case.id,
      type: "drift_acknowledged", actorType: "system", createdAt: new Date(2000) });
    await expect(transition(fixture)).resolves.toMatchObject({ case: { version: 2 } });
  });
  it("retains approved-version drift and the workflow-only exception", async () => {
    const fixture = await caseKernelFixture(db, { casePatch: { version: 2 } });
    await db.insert(pipelineCaseEvents).values({ companyId: fixture.company.id, caseId: fixture.case.id,
      type: "review_decided", actorType: "user", payload: { decision: "approve", approvedTransitionVersion: 1 } });
    await expect(transition(fixture, { toStageKey: "done" })).rejects.toMatchObject({ details: { code: "review_outdated" } });
    await expect(transition(fixture, { toStageKey: "done", actor: { type: "system" },
      transitionClass: "auto", reason: "children_terminal" })).resolves.toMatchObject({ case: { version: 3 } });
  });
  it("does not let an agent claim auto autonomy", async () => {
    const fixture = await caseKernelFixture(db);
    await expect(transition(fixture, { transitionClass: "auto" }))
      .rejects.toMatchObject({ details: { code: "autonomy_not_enabled" } });
  });
  it.each([false, true])("retains no-op/material review edits (%s)", async (material) => {
    const fixture = await caseKernelFixture(db, { stageKey: "review" });
    const result = await svc.reviewCase({ companyId: fixture.company.id, caseId: fixture.case.id,
      expectedVersion: 1, actor, decision: "approve", edits: { title: material ? "Changed" : fixture.case.title } });
    expect(result.case.version).toBe(material ? 3 : 2);
    expect(result.updateEvent === null).toBe(!material);
    expect(result.reviewEvent.payload.approvedTransitionVersion).toBe(result.case.version);
    expect((await events(fixture.case.id)).map((event) => event.type))
      .toEqual(material ? ["updated", "transitioned", "review_decided"] : ["transitioned", "review_decided"]);
  });
  it("requires a request-changes reason and preserves its original text", async () => {
    const fixture = await caseKernelFixture(db, { stageKey: "review" });
    const input = { companyId: fixture.company.id, caseId: fixture.case.id, expectedVersion: 1,
      actor, decision: "request_changes" as const };
    await expect(svc.reviewCase(input)).rejects.toMatchObject({ status: 422 });
    expect(await events(fixture.case.id)).toHaveLength(0);
    const result = await svc.reviewCase({ ...input, reason: "  revise this  " });
    expect(result.case.stageId).toBe(fixture.stages.get("draft")?.id);
    expect(result.reviewEvent.payload.reason).toBe("  revise this  ");
  });
});
