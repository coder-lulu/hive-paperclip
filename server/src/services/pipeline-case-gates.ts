import { and, asc, desc, eq, isNull, ne, or, sql } from "drizzle-orm";
import {
  pipelineCaseBlockers,
  pipelineCaseEvents,
  pipelineCases,
  pipelineStages,
} from "@paperclipai/db";
import { conflict } from "../errors.js";
import { normalizeStageConfig, stageConfig } from "./pipeline-stage-config.js";
import { childrenGateConfig } from "./pipeline-stage-breakdown.js";
import type { PipelineDb } from "./pipeline-case-types.js";

export async function assertNoOpenBlockers(db: PipelineDb, row: typeof pipelineCases.$inferSelect, toStage: typeof pipelineStages.$inferSelect) {
  if (toStage.kind !== "working" && toStage.kind !== "done") return;
  const blockers = await db
    .select({
      id: pipelineCases.id,
      caseKey: pipelineCases.caseKey,
      title: pipelineCases.title,
      terminalKind: pipelineCases.terminalKind,
    })
    .from(pipelineCaseBlockers)
    .innerJoin(pipelineCases, eq(pipelineCaseBlockers.blockedByCaseId, pipelineCases.id))
    .where(
      and(
        eq(pipelineCaseBlockers.companyId, row.companyId),
        eq(pipelineCaseBlockers.caseId, row.id),
        or(isNull(pipelineCases.terminalKind), ne(pipelineCases.terminalKind, "done")),
      ),
    );
  if (blockers.length > 0) {
    throw conflict("Pipeline case is blocked", { code: "blocked", blockers });
  }
}

export function expectedChildrenFromFields(fields: Record<string, unknown> | null | undefined) {
  const value = fields?.expectedChildren;
  if (typeof value === "number" && Number.isInteger(value) && value >= 0) return value;
  if (typeof value === "string" && /^\d+$/.test(value.trim())) return Number(value.trim());
  return null;
}

export async function listUnresolvedDriftEvents(db: PipelineDb, input: { companyId: string; caseId: string }) {
  const latestAck = await db
    .select({ createdAt: pipelineCaseEvents.createdAt })
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.companyId, input.companyId),
      eq(pipelineCaseEvents.caseId, input.caseId),
      eq(pipelineCaseEvents.type, "drift_acknowledged"),
    ))
    .orderBy(desc(pipelineCaseEvents.createdAt), desc(pipelineCaseEvents.id))
    .limit(1)
    .then((rows) => rows[0] ?? null);

  return db
    .select()
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.companyId, input.companyId),
      eq(pipelineCaseEvents.caseId, input.caseId),
      eq(pipelineCaseEvents.type, "upstream_drift"),
      latestAck ? sql`${pipelineCaseEvents.createdAt} > ${latestAck.createdAt.toISOString()}` : undefined,
    ))
    .orderBy(desc(pipelineCaseEvents.createdAt), desc(pipelineCaseEvents.id));
}

export async function assertStageTransitionGates(
  db: PipelineDb,
  current: typeof pipelineCases.$inferSelect,
  fromStage: typeof pipelineStages.$inferSelect,
  options: { skipChildrenTerminalGate?: boolean } = {},
) {
  const config = normalizeStageConfig(fromStage.kind, stageConfig(fromStage));
  const gate = childrenGateConfig(config);
  if (gate.requireChildrenTerminal && options.skipChildrenTerminalGate !== true) {
    const expectedChildren = expectedChildrenFromFields(current.fields);
    if (expectedChildren !== null && expectedChildren !== current.childCount) {
      throw conflict("Pipeline expected child count does not match created child cases", {
        code: "expected_children_mismatch",
        expectedChildren,
        childCount: current.childCount,
      });
    }
    if (current.childCount !== current.terminalChildCount) {
      const openChild = await db
        .select({
          id: pipelineCases.id,
          caseKey: pipelineCases.caseKey,
          title: pipelineCases.title,
          terminalKind: pipelineCases.terminalKind,
        })
        .from(pipelineCases)
        .where(and(
          eq(pipelineCases.companyId, current.companyId),
          eq(pipelineCases.parentCaseId, current.id),
          isNull(pipelineCases.terminalKind),
        ))
        .orderBy(asc(pipelineCases.createdAt))
        .limit(1)
        .then((rows) => rows[0] ?? null);
      throw conflict(
        openChild
          ? `Pipeline child case "${openChild.title}" is still open`
          : "Pipeline child cases are not all terminal",
        {
          code: "children_not_terminal",
          childCount: current.childCount,
          terminalChildCount: current.terminalChildCount,
          child: openChild,
        },
      );
    }
  }

  if (config.requireNoUnresolvedDrift === true) {
    const unresolvedDrift = await listUnresolvedDriftEvents(db, {
      companyId: current.companyId,
      caseId: current.id,
    });
    if (unresolvedDrift.length > 0) {
      const first = unresolvedDrift[0]!;
      const payload = first.payload as Record<string, unknown>;
      const upstream = typeof payload.upstreamCaseKey === "string"
        ? payload.upstreamCaseKey
        : typeof payload.upstreamCaseId === "string"
          ? payload.upstreamCaseId
          : "upstream case";
      throw conflict(`Pipeline upstream change from "${upstream}" is not acknowledged`, {
        code: "unresolved_drift",
        driftEventId: first.id,
        upstreamCaseId: typeof payload.upstreamCaseId === "string" ? payload.upstreamCaseId : null,
        upstreamCaseKey: typeof payload.upstreamCaseKey === "string" ? payload.upstreamCaseKey : null,
      });
    }
  }
}

export async function assertLatestReviewApprovalStillCurrent(
  db: PipelineDb,
  current: typeof pipelineCases.$inferSelect,
  fromStage: typeof pipelineStages.$inferSelect,
  toStage: typeof pipelineStages.$inferSelect,
  options: { allowWorkflowVersionDrift?: boolean } = {},
) {
  if (fromStage.kind === "review" || toStage.kind !== "done") return;
  const latestApproval = await db
    .select()
    .from(pipelineCaseEvents)
    .where(and(
      eq(pipelineCaseEvents.companyId, current.companyId),
      eq(pipelineCaseEvents.caseId, current.id),
      eq(pipelineCaseEvents.type, "review_decided"),
      sql`${pipelineCaseEvents.payload}->>'decision' = 'approve'`,
    ))
    .orderBy(desc(pipelineCaseEvents.createdAt), desc(pipelineCaseEvents.id))
    .limit(1)
    .then((rows) => rows[0] ?? null);
  if (!latestApproval) return;
  const payload = latestApproval.payload as Record<string, unknown>;
  const approvedVersion = typeof payload.approvedTransitionVersion === "number"
    ? payload.approvedTransitionVersion
    : typeof payload.approvedCaseVersion === "number"
      ? payload.approvedCaseVersion
      : null;
  if (approvedVersion === null || approvedVersion === current.version) return;
  if (options.allowWorkflowVersionDrift) {
    const materialUpdate = await db
      .select({ id: pipelineCaseEvents.id })
      .from(pipelineCaseEvents)
      .where(and(
        eq(pipelineCaseEvents.companyId, current.companyId),
        eq(pipelineCaseEvents.caseId, current.id),
        eq(pipelineCaseEvents.type, "updated"),
        sql`${pipelineCaseEvents.createdAt} > ${latestApproval.createdAt.toISOString()}`,
        sql`${pipelineCaseEvents.payload}->>'materialChanged' = 'true'`,
      ))
      .limit(1)
      .then((rows) => rows[0] ?? null);
    if (!materialUpdate) return;
  }
  throw conflict("Pipeline case changed since review approval; send it back through review before publishing", {
    code: "review_outdated",
    reviewEventId: latestApproval.id,
    approvedVersion,
    currentVersion: current.version,
  });
}
