import { and, eq, sql } from "drizzle-orm";
import { pipelineCases, pipelineStages, pipelines } from "@paperclipai/db";
import { conflict, notFound, unprocessable } from "../errors.js";
import { nowDate, writeCaseEvent } from "./pipeline-case-events.js";
import type { PipelineActor, PipelineDb } from "./pipeline-case-types.js";

export function hasValidLease(row: typeof pipelineCases.$inferSelect, now = nowDate()) {
  return Boolean(row.leaseToken && row.leaseExpiresAt && row.leaseExpiresAt.getTime() > now.getTime());
}

export function leaseOwner(row: typeof pipelineCases.$inferSelect) {
  if (row.leaseOwnerType === "agent") {
    return { type: "agent", agentId: row.leaseAgentId, expiresAt: row.leaseExpiresAt };
  }
  if (row.leaseOwnerType === "user") {
    return { type: "user", userId: row.leaseUserId, expiresAt: row.leaseExpiresAt };
  }
  return { type: row.leaseOwnerType, expiresAt: row.leaseExpiresAt };
}

export function actorOwnsLease(row: typeof pipelineCases.$inferSelect, actor: PipelineActor, leaseToken?: string | null) {
  if (!row.leaseToken) return true;
  if (leaseToken && leaseToken === row.leaseToken) return true;
  if (actor.type === "system") return true;
  if (actor.type === "agent") return row.leaseOwnerType === "agent" && row.leaseAgentId === actor.agentId;
  if (actor.type === "user") return row.leaseOwnerType === "user" && row.leaseUserId === actor.userId;
  return false;
}

export function conflictDetailsForCase(row: typeof pipelineCases.$inferSelect, stage?: typeof pipelineStages.$inferSelect | null) {
  return {
    code: "version_conflict",
    version: row.version,
    stage: stage ? { id: stage.id, key: stage.key, kind: stage.kind } : { id: row.stageId },
  };
}

export async function getPipelineOrThrow(db: PipelineDb, companyId: string, pipelineId: string) {
  const row = await db
    .select()
    .from(pipelines)
    .where(and(eq(pipelines.id, pipelineId), eq(pipelines.companyId, companyId)))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw notFound("Pipeline not found");
  return row;
}

export async function getStageOrThrow(db: PipelineDb, pipelineId: string, stageId: string) {
  const row = await db
    .select()
    .from(pipelineStages)
    .where(and(eq(pipelineStages.id, stageId), eq(pipelineStages.pipelineId, pipelineId)))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw notFound("Pipeline stage not found");
  return row;
}

export async function getStageByKeyOrThrow(db: PipelineDb, pipelineId: string, key: string) {
  const row = await db
    .select()
    .from(pipelineStages)
    .where(and(eq(pipelineStages.pipelineId, pipelineId), eq(pipelineStages.key, key)))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw notFound("Pipeline stage not found");
  return row;
}

export async function getCaseWithStageOrThrow(db: PipelineDb, companyId: string, caseId: string) {
  const row = await db
    .select({ case: pipelineCases, stage: pipelineStages, pipeline: pipelines })
    .from(pipelineCases)
    .innerJoin(pipelineStages, eq(pipelineCases.stageId, pipelineStages.id))
    .innerJoin(pipelines, eq(pipelineCases.pipelineId, pipelines.id))
    .where(and(eq(pipelineCases.id, caseId), eq(pipelineCases.companyId, companyId), eq(pipelines.companyId, companyId)))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw notFound("Pipeline case not found");
  return row;
}

export async function getCaseWithStageForUpdateOrThrow(db: PipelineDb, companyId: string, caseId: string) {
  const locked = await db.execute(sql<{ id: string }>`
    select id from pipeline_cases
    where company_id = ${companyId} and id = ${caseId}
    for update
  `);
  if (Array.from(locked).length === 0) throw notFound("Pipeline case not found");
  return getCaseWithStageOrThrow(db, companyId, caseId);
}

export async function expireLeaseIfNeeded(db: PipelineDb, row: typeof pipelineCases.$inferSelect, actor: PipelineActor) {
  const now = nowDate();
  if (!row.leaseToken || !row.leaseExpiresAt || row.leaseExpiresAt.getTime() > now.getTime()) {
    return row;
  }

  const [updated] = await db
    .update(pipelineCases)
    .set({
      leaseOwnerType: null,
      leaseAgentId: null,
      leaseUserId: null,
      leaseToken: null,
      leaseExpiresAt: null,
      updatedAt: now,
    })
    .where(and(eq(pipelineCases.id, row.id), eq(pipelineCases.leaseToken, row.leaseToken)))
    .returning();
  if (!updated) return row;

  await writeCaseEvent(db, {
    companyId: row.companyId,
    caseId: row.id,
    type: "lease_expired",
    actor,
    payload: { previousOwner: leaseOwner(row), expiredAt: now.toISOString() },
  });
  return updated;
}

export async function assertLeaseAvailable(
  db: PipelineDb,
  row: typeof pipelineCases.$inferSelect,
  actor: PipelineActor,
  leaseToken?: string | null,
) {
  const current = await expireLeaseIfNeeded(db, row, { type: "system" });
  if (hasValidLease(current) && !actorOwnsLease(current, actor, leaseToken)) {
    throw conflict("Pipeline case lease is held", { code: "lease_held", lease: leaseOwner(current) });
  }
  return current;
}

export async function getCaseOrThrow(db: PipelineDb, companyId: string, caseId: string) {
  const row = await db
    .select()
    .from(pipelineCases)
    .where(and(eq(pipelineCases.id, caseId), eq(pipelineCases.companyId, companyId)))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!row) throw notFound("Pipeline case not found");
  return row;
}

export async function assertValidParentCase(
  db: PipelineDb,
  input: { companyId: string; caseId?: string | null; parentCaseId?: string | null },
) {
  if (!input.parentCaseId) return null;
  if (input.caseId && input.parentCaseId === input.caseId) {
    throw conflict("Pipeline case parent cycle detected", { code: "parent_cycle" });
  }

  const parent = await getCaseOrThrow(db, input.companyId, input.parentCaseId);
  let current = parent;
  let depth = 1;
  while (current.parentCaseId) {
    if (input.caseId && current.parentCaseId === input.caseId) {
      throw conflict("Pipeline case parent cycle detected", { code: "parent_cycle" });
    }
    if (depth >= 32) {
      throw unprocessable("Pipeline case parent depth exceeds 32", { code: "parent_depth_exceeded" });
    }
    current = await getCaseOrThrow(db, input.companyId, current.parentCaseId);
    depth += 1;
  }
  if (depth >= 32) {
    throw unprocessable("Pipeline case parent depth exceeds 32", { code: "parent_depth_exceeded" });
  }
  return parent;
}
