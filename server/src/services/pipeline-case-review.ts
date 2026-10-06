import { pipelineAutomationExecutions, pipelineCaseEvents } from "@paperclipai/db";
import { unprocessable } from "../errors.js";
import { getCaseWithStageOrThrow } from "./pipeline-case-state.js";
import {
  reviewConfigForStage,
  assertActorCanApproveStageExit,
  targetStageKeyForReviewDecision,
} from "./pipeline-stage-config.js";
import { patchCaseContentInTransaction } from "./pipeline-case-content.js";
import { transitionCaseMutationInTransaction } from "./pipeline-case-transition.js";
import { writeCaseEvent } from "./pipeline-case-events.js";
import type { PipelineDb, PipelineCaseReviewInput } from "./pipeline-case-types.js";

export async function reviewCaseMutationInTransaction(
  tx: PipelineDb, input: PipelineCaseReviewInput,
  automationLedgers: Array<typeof pipelineAutomationExecutions.$inferSelect> = [],
) {
  const detail = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
  if (detail.stage.kind !== "review") {
    throw unprocessable("Pipeline case is not in a review stage", { code: "validation" });
  }
  const config = reviewConfigForStage(detail.stage);
  assertActorCanApproveStageExit(detail.stage, input.actor);
  const reasonRequired =
    (input.decision === "request_changes" && config.requireRequestChangesReason !== false) ||
    (input.decision === "reject" && config.requireRejectReason !== false);
  if (reasonRequired && !input.reason?.trim()) {
    throw unprocessable("Review decision reason is required", { code: "validation" });
  }
  const toStageKey = targetStageKeyForReviewDecision(config, input.decision);
  const suggestionId = detail.case.pendingSuggestion?.id ?? null;
  let expectedVersion = input.expectedVersion;
  let updateEvent: typeof pipelineCaseEvents.$inferSelect | null = null;
  const hasEdits = input.edits && Object.keys(input.edits).length > 0;

  if (hasEdits) {
    const updated = await patchCaseContentInTransaction(tx, {
      companyId: input.companyId,
      caseId: input.caseId,
      ...input.edits,
      expectedVersion,
      leaseToken: input.leaseToken,
      actor: input.actor,
    });
    expectedVersion = updated.case.version;
    updateEvent = updated.event;
  }

  const transitioned = await transitionCaseMutationInTransaction(tx, {
    companyId: input.companyId,
    caseId: input.caseId,
    toStageKey,
    expectedVersion,
    leaseToken: input.leaseToken,
    reason: input.reason,
    actor: input.actor,
    automationLedgers,
  });
  const reviewEvent = await writeCaseEvent(tx, {
    companyId: input.companyId,
    caseId: input.caseId,
    type: "review_decided",
    actor: input.actor,
    fromStageId: detail.stage.id,
    toStageId: transitioned.case.stageId,
    payload: {
      decision: input.decision,
      reason: input.reason ?? null,
      suggestionId,
      updateEventId: updateEvent?.id ?? null,
      transitionEventId: transitioned.event.id,
      approvedCaseVersion: input.decision === "approve" ? expectedVersion : null,
      approvedTransitionVersion: input.decision === "approve" ? transitioned.case.version : null,
    },
  });
  return { ...transitioned, updateEvent, reviewEvent };
}
