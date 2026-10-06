import { randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { PgDialect } from "drizzle-orm/pg-core";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  agents,
  companies,
  createDb,
  EMBEDDED_POSTGRES_TEST_TIMEOUT_MS,
  heartbeatRuns,
  issues,
} from "@paperclipai/db";
import { issueCheckoutOwnershipCondition } from "../services/issue-checkout-ownership.js";
import { issueService } from "../services/issues.js";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const selfAgentId = "00000000-0000-4000-8000-000000000001";
const otherAgentId = "00000000-0000-4000-8000-000000000002";
const selfRunId = "00000000-0000-4000-8000-000000000003";
const otherRunId = "00000000-0000-4000-8000-000000000004";

type OwnershipFixture = [
  name: string,
  assigneeAgentId: string | null,
  checkoutRunId: string | null,
  executionRunId: string | null,
  acceptsSelfRun: boolean,
  acceptsNullRun: boolean,
];

// These are the original initial-CAS boundaries, before stale adoption and the
// service's separate same-run idempotency branch. An unassigned issue's checkout
// column is deliberately not an independent gate in that original predicate.
const ownershipFixtures: OwnershipFixture[] = [
  ["unassigned / empty / empty", null, null, null, true, true],
  ["unassigned / empty / self", null, null, selfRunId, true, false],
  ["unassigned / empty / other", null, null, otherRunId, false, false],
  ["unassigned / self / empty", null, selfRunId, null, true, true],
  ["unassigned / self / self", null, selfRunId, selfRunId, true, false],
  ["unassigned / self / other", null, selfRunId, otherRunId, false, false],
  ["unassigned / other / empty", null, otherRunId, null, true, true],
  ["unassigned / other / self", null, otherRunId, selfRunId, true, false],
  ["unassigned / other / other", null, otherRunId, otherRunId, false, false],
  ["self / empty / empty", selfAgentId, null, null, true, true],
  ["self / empty / self", selfAgentId, null, selfRunId, true, false],
  ["self / empty / other", selfAgentId, null, otherRunId, false, false],
  ["self / self / empty", selfAgentId, selfRunId, null, true, false],
  ["self / self / self", selfAgentId, selfRunId, selfRunId, true, false],
  ["self / self / other", selfAgentId, selfRunId, otherRunId, false, false],
  ["self / other / empty", selfAgentId, otherRunId, null, false, false],
  ["self / other / self", selfAgentId, otherRunId, selfRunId, false, false],
  ["self / other / other", selfAgentId, otherRunId, otherRunId, false, false],
  ["other / empty / empty", otherAgentId, null, null, false, false],
  ["other / empty / self", otherAgentId, null, selfRunId, false, false],
  ["other / empty / other", otherAgentId, null, otherRunId, false, false],
  ["other / self / empty", otherAgentId, selfRunId, null, false, false],
  ["other / self / self", otherAgentId, selfRunId, selfRunId, false, false],
  ["other / self / other", otherAgentId, selfRunId, otherRunId, false, false],
  ["other / other / empty", otherAgentId, otherRunId, null, false, false],
  ["other / other / self", otherAgentId, otherRunId, selfRunId, false, false],
  ["other / other / other", otherAgentId, otherRunId, otherRunId, false, false],
];

describe("issue checkout ownership SQL", () => {
  const dialect = new PgDialect();

  it("binds actor and run values rather than interpolating them into SQL", () => {
    const agentId = "agent' OR true --";
    const runId = "run' OR true --";
    const query = dialect.sqlToQuery(issueCheckoutOwnershipCondition(agentId, runId));

    expect(query.sql).not.toContain(agentId);
    expect(query.sql).not.toContain(runId);
    expect(query.params).toEqual([agentId, runId, runId]);
  });

  it("uses null-run IS NULL gates without adding a null equality parameter", () => {
    const query = dialect.sqlToQuery(issueCheckoutOwnershipCondition(selfAgentId, null));

    expect(query.params).toEqual([selfAgentId]);
    expect(query.sql).toContain('"issues"."checkout_run_id" is null');
    expect(query.sql).toContain('"issues"."execution_run_id" is null');
  });
});

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

describeEmbeddedPostgres("issue checkout ownership regression", () => {
  let db: ReturnType<typeof createDb>;
  let testDatabase: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    testDatabase = await startEmbeddedPostgresTestDatabase("paperclip-issue-checkout-ownership-");
    db = createDb(testDatabase.connectionString);
  }, EMBEDDED_POSTGRES_TEST_TIMEOUT_MS);

  afterAll(async () => {
    await testDatabase?.cleanup();
  });

  it.each(ownershipFixtures)(
    "preserves initial CAS with a run: %s",
    async (_name, assigneeAgentId, checkoutRunId, executionRunId, acceptsSelfRun) => {
      const rows = await db.execute(sql`
        select 1
        from (values (${assigneeAgentId}, ${checkoutRunId}, ${executionRunId}))
          as "issues" ("assignee_agent_id", "checkout_run_id", "execution_run_id")
        where ${issueCheckoutOwnershipCondition(selfAgentId, selfRunId)}
      `);

      expect(rows.length > 0).toBe(acceptsSelfRun);
    },
  );

  it.each(ownershipFixtures)(
    "preserves initial CAS without a run: %s",
    async (_name, assigneeAgentId, checkoutRunId, executionRunId, _acceptsSelfRun, acceptsNullRun) => {
      const rows = await db.execute(sql`
        select 1
        from (values (${assigneeAgentId}, ${checkoutRunId}, ${executionRunId}))
          as "issues" ("assignee_agent_id", "checkout_run_id", "execution_run_id")
        where ${issueCheckoutOwnershipCondition(selfAgentId, null)}
      `);

      expect(rows.length > 0).toBe(acceptsNullRun);
    },
  );

  it.each([
    { name: "the same run", withRun: true },
    { name: "a null run", withRun: false },
  ])("keeps existing checkout self-idempotent for $name", async ({ withRun }) => {
    const [company] = await db.insert(companies).values({
      name: "Checkout ownership regression",
      issuePrefix: `P${randomUUID().replace(/-/g, "").slice(0, 6).toUpperCase()}`,
    }).returning();
    if (!company) throw new Error("Company fixture was not inserted");

    const [agent] = await db.insert(agents).values({
      companyId: company.id,
      name: "Checkout agent",
      role: "engineer",
      status: "idle",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    }).returning();
    if (!agent) throw new Error("Agent fixture was not inserted");

    let checkoutRunId: string | null = null;
    if (withRun) {
      const [run] = await db.insert(heartbeatRuns).values({
        companyId: company.id,
        agentId: agent.id,
        status: "running",
      }).returning();
      if (!run) throw new Error("Run fixture was not inserted");
      checkoutRunId = run.id;
    }

    const [issue] = await db.insert(issues).values({
      companyId: company.id,
      title: "Checkout twice",
      status: "todo",
      priority: "medium",
      assigneeAgentId: agent.id,
    }).returning();
    if (!issue) throw new Error("Issue fixture was not inserted");

    const service = issueService(db);
    const first = await service.checkout(issue.id, agent.id, ["todo"], checkoutRunId);
    const second = await service.checkout(issue.id, agent.id, ["todo"], checkoutRunId);

    expect(second.id).toBe(first.id);
    expect(second.status).toBe("in_progress");
    expect(second.assigneeAgentId).toBe(agent.id);
    expect(second.checkoutRunId).toBe(checkoutRunId);
    expect(second.executionRunId).toBe(checkoutRunId);
    expect(second.startedAt).toEqual(first.startedAt);
    expect(second.updatedAt).toEqual(first.updatedAt);
  });
});
