import { randomUUID } from "node:crypto";
import {
  companies,
  createDb,
  EMBEDDED_POSTGRES_TEST_TIMEOUT_MS,
  executionWorkspaces,
  issueRelations,
  issues,
  issueTreeHolds,
  projects,
  projectWorkspaces,
  workspaceOperations,
} from "@paperclipai/db";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { getActivePauseHoldGate } from "../services/issue-checkout-admission.js";
import { listIssueDependencyReadinessMap } from "../services/issue-dependency-readiness.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const support = await getEmbeddedPostgresTestSupport();
const describePostgres = support.supported ? describe : describe.skip;

describePostgres("original checkout admission read guards", () => {
  let db: ReturnType<typeof createDb>;
  let testDatabase: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    testDatabase = await startEmbeddedPostgresTestDatabase("paperclip-checkout-admission-");
    db = createDb(testDatabase.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);

  afterAll(async () => {
    await testDatabase?.cleanup();
  });

  async function companyFixture() {
    const [company] = await db.insert(companies).values({
      name: "Checkout admission guards",
      issuePrefix: `P${randomUUID().replace(/-/g, "").slice(0, 6).toUpperCase()}`,
    }).returning();
    if (!company) throw new Error("Company fixture was not inserted");
    return company;
  }

  async function issueFixture(companyId: string, parentId: string | null = null) {
    const [issue] = await db.insert(issues).values({
      companyId,
      parentId,
      title: "Guard fixture",
      status: "todo",
      priority: "medium",
    }).returning();
    if (!issue) throw new Error("Issue fixture was not inserted");
    return issue;
  }

  it("returns the nearest active ancestor pause and preserves root metadata", async () => {
    const company = await companyFixture();
    const root = await issueFixture(company.id);
    const parent = await issueFixture(company.id, root.id);
    const child = await issueFixture(company.id, parent.id);
    const [rootHold, parentHold] = await db.insert(issueTreeHolds).values([
      { companyId: company.id, rootIssueId: root.id, mode: "pause", status: "active", reason: "Root pause", createdByActorType: "system" },
      { companyId: company.id, rootIssueId: parent.id, mode: "pause", status: "active", reason: "Parent pause", createdByActorType: "system" },
    ]).returning();
    if (!rootHold || !parentHold) throw new Error("Pause fixtures were not inserted");

    const childGate = await getActivePauseHoldGate(db, company.id, child.id);
    expect(childGate).toMatchObject({
      holdId: parentHold.id, rootIssueId: parent.id, issueId: child.id,
      isRoot: false, mode: "pause", reason: "Parent pause", releasePolicy: null,
    });
    const rootGate = await getActivePauseHoldGate(db, company.id, root.id);
    expect(rootGate).toMatchObject({ holdId: rootHold.id, isRoot: true });
  });

  it("ignores other-company, released, and cancel holds", async () => {
    const company = await companyFixture();
    const otherCompany = await companyFixture();
    const issue = await issueFixture(company.id);
    const otherIssue = await issueFixture(otherCompany.id);
    await db.insert(issueTreeHolds).values([
      { companyId: otherCompany.id, rootIssueId: otherIssue.id, mode: "pause", status: "active", createdByActorType: "system" },
      { companyId: company.id, rootIssueId: issue.id, mode: "pause", status: "released", createdByActorType: "system" },
      { companyId: company.id, rootIssueId: issue.id, mode: "cancel", status: "active", createdByActorType: "system" },
    ]);

    expect(await getActivePauseHoldGate(db, company.id, issue.id)).toBeNull();
  });

  it("retains the original 100-node ancestor boundary", async () => {
    const company = await companyFixture();
    const ids = Array.from({ length: 101 }, () => randomUUID());
    const childId = ids[0];
    const rootId = ids[100];
    if (!childId || !rootId) throw new Error("Ancestor fixture IDs are missing");
    await db.insert(issues).values(ids.map((id, index) => ({
      id, companyId: company.id, parentId: ids[index + 1] ?? null,
      title: `Ancestor ${index}`, status: "todo", priority: "medium",
    })));
    await db.insert(issueTreeHolds).values({
      companyId: company.id, rootIssueId: rootId,
      mode: "pause", status: "active", createdByActorType: "system",
    });

    expect(await getActivePauseHoldGate(db, company.id, childId)).toBeNull();
    const withinBoundaryId = ids[1];
    if (!withinBoundaryId) throw new Error("Boundary fixture ID is missing");
    expect(await getActivePauseHoldGate(db, company.id, withinBoundaryId))
      .toMatchObject({ rootIssueId: rootId, isRoot: false });
  });

  it.each(["todo", "cancelled", "done"])("preserves blocker disposition %s", async (status) => {
    const company = await companyFixture();
    const target = await issueFixture(company.id);
    const [blocker] = await db.insert(issues).values({
      companyId: company.id, title: "Blocker", priority: "medium", status,
    }).returning();
    if (!blocker) throw new Error("Blocker fixture was not inserted");
    await db.insert(issueRelations).values({
      companyId: company.id, issueId: blocker.id, relatedIssueId: target.id, type: "blocks",
    });

    const result = await listIssueDependencyReadinessMap(db, company.id, [target.id, target.id, ""]);
    expect(result.size).toBe(1);
    expect(result.get(target.id)).toMatchObject({
      blockerIssueIds: [blocker.id],
      unresolvedBlockerIssueIds: status === "done" ? [] : [blocker.id],
      unresolvedBlockerCount: status === "done" ? 0 : 1,
      pendingFinalizeBlockerIssueIds: [],
      allBlockersDone: status === "done", isDependencyReady: status === "done",
    });
    const otherCompany = await companyFixture();
    expect((await listIssueDependencyReadinessMap(db, otherCompany.id, [target.id])).get(target.id))
      .toMatchObject({ blockerIssueIds: [], isDependencyReady: true });
  });

  type OperationFixture = {
    issue: "blocker" | "none" | "other";
    status: string;
    at: number;
  };
  type FinalizeFixture = { name: string; operations: OperationFixture[]; ready: boolean };
  const finalizeFixtures: FinalizeFixture[] = [
    { name: "no operations", operations: [], ready: true },
    { name: "attributed running finalize", operations: [{ issue: "blocker", status: "running", at: 1 }], ready: false },
    { name: "attributed failed finalize", operations: [{ issue: "blocker", status: "failed", at: 1 }], ready: false },
    { name: "attributed successful finalize", operations: [{ issue: "blocker", status: "succeeded", at: 1 }], ready: true },
    { name: "unattributed fallback", operations: [{ issue: "none", status: "running", at: 1 }], ready: false },
    { name: "unrelated attributed operation", operations: [{ issue: "other", status: "running", at: 1 }], ready: true },
    { name: "later shared success", operations: [
      { issue: "blocker", status: "running", at: 1 }, { issue: "other", status: "succeeded", at: 2 },
    ], ready: true },
    { name: "newer unattributed failure retains attributed success", operations: [
      { issue: "blocker", status: "succeeded", at: 1 }, { issue: "none", status: "failed", at: 2 },
    ], ready: true },
  ];

  it.each(finalizeFixtures)("preserves finalize barrier: $name", async ({ operations, ready }) => {
    const company = await companyFixture();
    const target = await issueFixture(company.id);
    const otherIssue = await issueFixture(company.id);
    const [project] = await db.insert(projects).values({
      companyId: company.id, name: "Finalize project", status: "in_progress",
    }).returning();
    if (!project) throw new Error("Project fixture was not inserted");
    const [projectWorkspace] = await db.insert(projectWorkspaces).values({
      companyId: company.id, projectId: project.id, name: "Source", isPrimary: true,
      sharedWorkspaceKey: randomUUID(),
    }).returning();
    if (!projectWorkspace) throw new Error("Project workspace fixture was not inserted");
    const [workspace] = await db.insert(executionWorkspaces).values({
      companyId: company.id, projectId: project.id, projectWorkspaceId: projectWorkspace.id,
      mode: "isolated_workspace", strategyType: "git_worktree", name: "Finalize workspace",
      status: "active", providerType: "git_worktree",
    }).returning();
    if (!workspace) throw new Error("Execution workspace fixture was not inserted");
    const [blocker] = await db.insert(issues).values({
      companyId: company.id, title: "Done blocker", status: "done", priority: "medium",
      executionWorkspaceId: workspace.id,
    }).returning();
    if (!blocker) throw new Error("Finalize blocker fixture was not inserted");
    await db.insert(issueRelations).values({
      companyId: company.id, issueId: blocker.id, relatedIssueId: target.id, type: "blocks",
    });
    if (operations.length > 0) {
      await db.insert(workspaceOperations).values(operations.map((operation) => ({
        companyId: company.id, executionWorkspaceId: workspace.id,
        issueId: operation.issue === "blocker" ? blocker.id : operation.issue === "other" ? otherIssue.id : null,
        phase: "workspace_finalize", status: operation.status,
        startedAt: new Date(Date.UTC(2026, 0, 1, 0, 0, operation.at)),
      })));
    }

    const result = (await listIssueDependencyReadinessMap(db, company.id, [target.id])).get(target.id);
    expect(result).toMatchObject({
      allBlockersDone: ready, isDependencyReady: ready,
      unresolvedBlockerIssueIds: ready ? [] : [blocker.id],
      pendingFinalizeBlockerIssueIds: ready ? [] : [blocker.id],
    });
  });
});
