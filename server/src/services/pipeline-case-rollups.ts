import { and, desc, eq, inArray, isNull, ne, or, sql } from "drizzle-orm";
import {
  pipelineCaseBlockers,
  pipelineCaseEvents,
  pipelineCases,
  pipelineStages,
} from "@paperclipai/db";
import { notFound } from "../errors.js";
import {
  nowDate,
  writeCaseEvent,
  postSystemCommentOnLinkedIssues,
} from "./pipeline-case-events.js";
import { getCaseWithStageOrThrow } from "./pipeline-case-state.js";
import type { PipelineDb } from "./pipeline-case-types.js";

export async function adjustParentCounts(
  db: PipelineDb,
  input: { parentCaseId: string | null | undefined; childDelta?: number; terminalChildDelta?: number },
) {
  if (!input.parentCaseId) return;
  const patch: Partial<typeof pipelineCases.$inferInsert> = { updatedAt: nowDate() };
  if (input.childDelta) {
    patch.childCount = sql`${pipelineCases.childCount} + ${input.childDelta}` as unknown as number;
  }
  if (input.terminalChildDelta) {
    patch.terminalChildCount = sql`${pipelineCases.terminalChildCount} + ${input.terminalChildDelta}` as unknown as number;
  }
  if (!input.childDelta && !input.terminalChildDelta) return;
  await db.update(pipelineCases).set(patch).where(eq(pipelineCases.id, input.parentCaseId));
}

export async function computeCaseRollup(db: PipelineDb, companyId: string, caseId: string) {
  const rows = await db.execute(sql<{
    id: string;
    terminal_kind: string | null;
  }>`
    with recursive subtree as (
      select id, terminal_kind from pipeline_cases where company_id = ${companyId} and id = ${caseId}
      union all
      select child.id, child.terminal_kind
      from pipeline_cases child
      join subtree parent on child.parent_case_id = parent.id
      where child.company_id = ${companyId}
    )
    select id, terminal_kind from subtree
  `);
  const items = Array.from(rows);
  if (items.length === 0) throw notFound("Pipeline case not found");
  const descendants = items.slice(1);
  const done = descendants.filter((item) => item.terminal_kind === "done").length;
  const cancelled = descendants.filter((item) => item.terminal_kind === "cancelled").length;
  const open = descendants.filter((item) => item.terminal_kind !== "done" && item.terminal_kind !== "cancelled").length;
  return { total: descendants.length, done, cancelled, open, complete: open === 0 };
}

export async function hasBlockersResolvedForLatestBlockerSet(db: PipelineDb, caseId: string) {
  const latestBlockersSet = await db
    .select({ createdAt: pipelineCaseEvents.createdAt })
    .from(pipelineCaseEvents)
    .where(and(eq(pipelineCaseEvents.caseId, caseId), eq(pipelineCaseEvents.type, "blockers_set")))
    .orderBy(desc(pipelineCaseEvents.createdAt))
    .limit(1)
    .then((rows) => rows[0] ?? null);

  const row = await db
    .select({ id: pipelineCaseEvents.id })
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.caseId, caseId),
      eq(pipelineCaseEvents.type, "blockers_resolved"),
      latestBlockersSet ? sql`${pipelineCaseEvents.createdAt} > ${latestBlockersSet.createdAt.toISOString()}` : undefined,
    ))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  return Boolean(row);
}

export async function hasChildrenTerminalEventForRollup(
  db: PipelineDb,
  caseId: string,
  stageId: string,
  rollup: Awaited<ReturnType<typeof computeCaseRollup>>,
) {
  const stageEntry = await db
    .select({ createdAt: pipelineCaseEvents.createdAt })
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.caseId, caseId),
      inArray(pipelineCaseEvents.type, ["ingested", "transitioned", "automation_retry_dispatched"]),
      eq(pipelineCaseEvents.toStageId, stageId),
    ))
    .orderBy(desc(pipelineCaseEvents.createdAt))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  const row = await db
    .select({ id: pipelineCaseEvents.id })
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.caseId, caseId),
      eq(pipelineCaseEvents.type, "children_terminal"),
      sql`${pipelineCaseEvents.payload} -> 'rollup' = ${JSON.stringify(rollup)}::jsonb`,
      stageEntry ? sql`${pipelineCaseEvents.createdAt} > ${stageEntry.createdAt.toISOString()}::timestamptz` : undefined,
    ))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  return Boolean(row);
}

export async function getAncestorCases(db: PipelineDb, companyId: string, parentCaseId: string | null | undefined) {
  const ancestors: Array<{
    case: typeof pipelineCases.$inferSelect;
    stage: typeof pipelineStages.$inferSelect;
  }> = [];
  let nextId = parentCaseId ?? null;
  let depth = 0;
  while (nextId) {
    if (depth >= 32) break;
    const row = await getCaseWithStageOrThrow(db, companyId, nextId);
    ancestors.push(row);
    nextId = row.case.parentCaseId;
    depth += 1;
  }
  return ancestors;
}

export async function handleBlockersResolved(db: PipelineDb, companyId: string, blockerCaseId: string) {
  const blockedRows = await db
    .select({ caseId: pipelineCaseBlockers.caseId })
    .from(pipelineCaseBlockers)
    .where(and(eq(pipelineCaseBlockers.companyId, companyId), eq(pipelineCaseBlockers.blockedByCaseId, blockerCaseId)));

  for (const blocked of blockedRows) {
    const [{ count }] = await db
      .select({ count: sql<number>`count(*)::int` })
      .from(pipelineCaseBlockers)
      .innerJoin(pipelineCases, eq(pipelineCaseBlockers.blockedByCaseId, pipelineCases.id))
      .where(and(
        eq(pipelineCaseBlockers.companyId, companyId),
        eq(pipelineCaseBlockers.caseId, blocked.caseId),
        or(isNull(pipelineCases.terminalKind), ne(pipelineCases.terminalKind, "done")),
      ));
    if ((count ?? 0) > 0 || await hasBlockersResolvedForLatestBlockerSet(db, blocked.caseId)) continue;
    await writeCaseEvent(db, {
      companyId,
      caseId: blocked.caseId,
      type: "blockers_resolved",
      actor: { type: "system" },
      payload: { resolvedByCaseId: blockerCaseId },
    });
    await postSystemCommentOnLinkedIssues(db, {
      companyId,
      caseId: blocked.caseId,
      roles: ["work"],
      body: `Pipeline blockers resolved for case ${blocked.caseId}. The case can be retried now that blocker ${blockerCaseId} is done.`,
    });
  }
}
