import { and, eq } from "drizzle-orm";
import { pipelineAutomationExecutions, pipelineCases, pipelineTransitions } from "@paperclipai/db";
import { conflict, unprocessable } from "../errors.js";
import {
  assertLeaseAvailable,
  conflictDetailsForCase,
  getCaseWithStageForUpdateOrThrow,
  getCaseWithStageOrThrow,
  getStageOrThrow,
  getStageByKeyOrThrow,
} from "./pipeline-case-state.js";
import {
  assertStageEnabled,
  assertActorCanApproveStageExit,
  stageConfig,
} from "./pipeline-stage-config.js";
import {
  assertStageTransitionGates,
  assertLatestReviewApprovalStillCurrent,
  assertNoOpenBlockers,
} from "./pipeline-case-gates.js";
import {
  nowDate,
  isTerminalKind,
  terminalKindForStage,
  writeCaseEvent,
  eventActorPayload,
} from "./pipeline-case-events.js";
import { enqueueStageAutomationLedger } from "./pipeline-stage-ledger.js";
import { adjustParentCounts, handleBlockersResolved } from "./pipeline-case-rollups.js";
import { handleChildrenTerminal, maybeAutoAdvanceOnStageEntry } from "./pipeline-case-children.js";
import type { PipelineActor, PipelineDb } from "./pipeline-case-types.js";

export async function transitionCaseMutationInTransaction(
  tx: PipelineDb,
  input: {
    companyId: string;
    caseId: string;
    toStageId?: string;
    toStageKey?: string;
    expectedVersion: number;
    leaseToken?: string | null;
    actor: PipelineActor;
    transitionClass?: "manual" | "suggested" | "auto";
    suggestionId?: string;
    reason?: string | null;
    force?: boolean;
    automationLedgers?: Array<typeof pipelineAutomationExecutions.$inferSelect>;
    autoAdvanceVisitedStageIds?: Set<string>;
    skipChildrenTerminalGate?: boolean;
  },
) {
  if (input.transitionClass === "auto" && input.actor.type !== "system") {
    throw unprocessable("Pipeline auto autonomy is not enabled", { code: "autonomy_not_enabled" });
  }
  const { case: existing, stage: fromStage, pipeline } = await getCaseWithStageForUpdateOrThrow(tx, input.companyId, input.caseId);
  if (pipeline.archivedAt) throw unprocessable("Pipeline is archived", { code: "pipeline_archived" });
  const current = await assertLeaseAvailable(tx, existing, input.actor, input.leaseToken);
  if (current.version !== input.expectedVersion) {
    throw conflict("Pipeline case version conflict", conflictDetailsForCase(current, fromStage));
  }

  const toStage = input.toStageId
    ? await getStageOrThrow(tx, current.pipelineId, input.toStageId)
    : await getStageByKeyOrThrow(tx, current.pipelineId, input.toStageKey ?? "");
  assertStageEnabled(toStage, "transition");
  if (fromStage.id !== toStage.id) {
    assertActorCanApproveStageExit(fromStage, input.actor);
    await assertStageTransitionGates(tx, current, fromStage, { skipChildrenTerminalGate: input.skipChildrenTerminalGate });
    await assertLatestReviewApprovalStillCurrent(tx, current, fromStage, toStage, {
      allowWorkflowVersionDrift: input.transitionClass === "auto" && input.reason === "children_terminal",
    });
  }
  const toConfig = stageConfig(toStage);
  if (toConfig.autonomy === "auto") {
    throw unprocessable("Pipeline auto autonomy is not enabled", { code: "autonomy_not_enabled" });
  }
  let forcedTransition = false;
  if (pipeline.enforceTransitions && fromStage.id !== toStage.id) {
    const allowed = await tx
      .select({ id: pipelineTransitions.id })
      .from(pipelineTransitions)
      .where(
        and(
          eq(pipelineTransitions.pipelineId, current.pipelineId),
          eq(pipelineTransitions.fromStageId, fromStage.id),
          eq(pipelineTransitions.toStageId, toStage.id),
        ),
      )
      .limit(1)
      .then((rows) => rows[0] ?? null);
    if (!allowed) {
      const reason = input.reason?.trim() ?? "";
      if (input.force !== true || reason.length === 0) {
        throw conflict("Pipeline transition is not allowed", { code: "transition_not_allowed" });
      }
      forcedTransition = true;
    }
  }
  await assertNoOpenBlockers(tx, current, toStage);

  const enteringTerminal = terminalKindForStage(toStage.kind);
  const [updated] = await tx
    .update(pipelineCases)
    .set({
      stageId: toStage.id,
      version: current.version + 1,
      terminalKind: enteringTerminal,
      terminalAt: enteringTerminal ? nowDate() : null,
      pendingSuggestion: input.suggestionId === current.pendingSuggestion?.id ? null : current.pendingSuggestion,
      leaseOwnerType: enteringTerminal ? null : current.leaseOwnerType,
      leaseAgentId: enteringTerminal ? null : current.leaseAgentId,
      leaseUserId: enteringTerminal ? null : current.leaseUserId,
      leaseToken: enteringTerminal ? null : current.leaseToken,
      leaseExpiresAt: enteringTerminal ? null : current.leaseExpiresAt,
      updatedAt: nowDate(),
    })
    .where(and(eq(pipelineCases.id, current.id), eq(pipelineCases.version, current.version)))
    .returning();
  if (!updated) {
    const latest = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
    throw conflict("Pipeline case version conflict", conflictDetailsForCase(latest.case, latest.stage));
  }

  const event = await writeCaseEvent(tx, {
    companyId: input.companyId,
    caseId: current.id,
    type: "transitioned",
    actor: input.actor,
    fromStageId: fromStage.id,
    toStageId: toStage.id,
    payload: {
      previousVersion: current.version,
      version: updated.version,
      suggestionId: input.suggestionId ?? null,
      reason: input.reason ?? null,
      transitionClass: input.transitionClass ?? "manual",
    },
  });
  if (forcedTransition) {
    await writeCaseEvent(tx, {
      companyId: input.companyId,
      caseId: current.id,
      type: "transition_forced",
      actor: input.actor,
      fromStageId: fromStage.id,
      toStageId: toStage.id,
      payload: {
        fromStageId: fromStage.id,
        toStageId: toStage.id,
        reason: input.reason!.trim(),
        actor: eventActorPayload(input.actor),
      },
    });
  }
  const ledger = await enqueueStageAutomationLedger(tx, {
    companyId: input.companyId,
    caseId: current.id,
    stage: toStage,
    eventId: event.id,
  });
  if (ledger) input.automationLedgers?.push(ledger);
  const wasTerminal = isTerminalKind(current.terminalKind);
  const isTerminal = isTerminalKind(updated.terminalKind);
  if (current.parentCaseId && wasTerminal !== isTerminal) {
    await adjustParentCounts(tx, {
      parentCaseId: current.parentCaseId,
      terminalChildDelta: isTerminal ? 1 : -1,
    });
  }
  if (!wasTerminal && updated.terminalKind === "done") {
    await handleBlockersResolved(tx, input.companyId, current.id);
  }
  if (!wasTerminal && isTerminal) {
    await handleChildrenTerminal(tx, input.companyId, current.parentCaseId, input.automationLedgers);
  }
  if (!isTerminal) {
    await maybeAutoAdvanceOnStageEntry(tx, {
      companyId: input.companyId,
      caseRow: updated,
      stage: toStage,
      automationLedgers: input.automationLedgers,
      visitedStageIds: input.autoAdvanceVisitedStageIds,
    });
  }
  return { case: updated, event, automationLedger: ledger };
}
