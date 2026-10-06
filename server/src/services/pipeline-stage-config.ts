import { pipelineStages } from "@paperclipai/db";
import { HttpError, unprocessable } from "../errors.js";
import { readBreakdownConfig } from "./pipeline-stage-breakdown.js";
import type {
  PipelineActor,
  PipelineStageConfig,
  PipelineStageKind,
  PipelineReviewDecision,
} from "./pipeline-case-types.js";

export function stageConfig(stage: typeof pipelineStages.$inferSelect): PipelineStageConfig {
  return (stage.config ?? {}) as PipelineStageConfig;
}

export function persistedStageConfig(config?: PipelineStageConfig | null): PipelineStageConfig {
  const {
    automation: _automation,
    assigneeAgentId: _assigneeAgentId,
    ...rest
  } = { ...(config ?? {}) } as PipelineStageConfig & { assigneeAgentId?: unknown };
  return rest as PipelineStageConfig;
}

export function normalizeStageConfig(kind: PipelineStageKind | string, config?: PipelineStageConfig | null): PipelineStageConfig {
  const { reviewerKind, ...rest } = persistedStageConfig(config);
  const next = rest as PipelineStageConfig;

  if (next.disabled !== undefined && typeof next.disabled !== "boolean") {
    throw unprocessable("Stage disabled must be boolean", { code: "validation" });
  }

  if (next.requireApproval !== undefined && typeof next.requireApproval !== "boolean") {
    throw unprocessable("Stage requireApproval must be boolean", { code: "validation" });
  }
  if (next.requireChildrenTerminal !== undefined && typeof next.requireChildrenTerminal !== "boolean") {
    throw unprocessable("Stage requireChildrenTerminal must be boolean", { code: "validation" });
  }
  if (next.requireNoUnresolvedDrift !== undefined && typeof next.requireNoUnresolvedDrift !== "boolean") {
    throw unprocessable("Stage requireNoUnresolvedDrift must be boolean", { code: "validation" });
  }
  if (next.breakdown !== undefined) {
    if (!next.breakdown || typeof next.breakdown !== "object" || Array.isArray(next.breakdown)) {
      throw unprocessable("Stage breakdown must be an object", { code: "validation" });
    }
    const breakdown = readBreakdownConfig(next);
    next.breakdown = {
      ...(next.breakdown as Record<string, unknown>),
      targetPipelineId: breakdown!.targetPipelineId,
      targetStageKey: breakdown!.targetStageKey,
      pieceNoun: breakdown!.pieceNoun,
      carryOverPolicy: breakdown!.carryOverPolicy,
      inheritFields: breakdown!.inheritFields,
      ...(breakdown!.advanceTo ? { advanceTo: breakdown!.advanceTo } : {}),
      waitForPieces: breakdown!.waitForPieces,
      ...(breakdown!.whenFinishedMoveTo ? { whenFinishedMoveTo: breakdown!.whenFinishedMoveTo } : {}),
    };
  }

  if (reviewerKind !== undefined && reviewerKind !== "human" && reviewerKind !== "any") {
    throw unprocessable("Review stage reviewerKind must be human or any", { code: "validation" });
  }

  const legacyRequiresApproval = reviewerKind === "human" ? true : reviewerKind === "any" ? false : undefined;
  const requireApproval = legacyRequiresApproval ?? next.requireApproval ?? kind === "review";
  const approver = normalizeStageApprover(next.approver, requireApproval);
  next.requireApproval = requireApproval;
  next.approver = approver;

  if (kind !== "review") return next;

  if (typeof next.approveToStageKey !== "string" || next.approveToStageKey.trim().length === 0) {
    throw unprocessable("Review stages require approveToStageKey", { code: "validation" });
  }
  if (typeof next.rejectToStageKey !== "string" || next.rejectToStageKey.trim().length === 0) {
    throw unprocessable("Review stages require rejectToStageKey", { code: "validation" });
  }
  if (
    next.requestChangesToStageKey !== undefined &&
    (typeof next.requestChangesToStageKey !== "string" || next.requestChangesToStageKey.trim().length === 0)
  ) {
    throw unprocessable("Review stage requestChangesToStageKey must be a non-empty string", { code: "validation" });
  }
  if (next.requireRejectReason !== undefined && typeof next.requireRejectReason !== "boolean") {
    throw unprocessable("Review stage requireRejectReason must be boolean", { code: "validation" });
  }
  if (next.requireRequestChangesReason !== undefined && typeof next.requireRequestChangesReason !== "boolean") {
    throw unprocessable("Review stage requireRequestChangesReason must be boolean", { code: "validation" });
  }
  return {
    ...next,
    approveToStageKey: next.approveToStageKey.trim(),
    rejectToStageKey: next.rejectToStageKey.trim(),
    ...(next.requestChangesToStageKey !== undefined ? { requestChangesToStageKey: next.requestChangesToStageKey.trim() } : {}),
    requireRejectReason: next.requireRejectReason ?? true,
    requireRequestChangesReason: next.requireRequestChangesReason ?? true,
    requireApproval,
    approver,
  };
}

export function reviewConfigForStage(stage: typeof pipelineStages.$inferSelect) {
  const config = normalizeStageConfig(stage.kind, stageConfig(stage));
  const reviewerKind: PipelineStageConfig["reviewerKind"] = config.requireApproval === true ? "human" : "any";
  return {
    ...config,
    reviewerKind,
  };
}

export function normalizeStageApprover(
  approver: PipelineStageConfig["approver"] | undefined,
  requireApproval: boolean,
): NonNullable<PipelineStageConfig["approver"]> {
  if (approver !== undefined && (typeof approver !== "object" || approver === null || Array.isArray(approver))) {
    throw unprocessable("Stage approver must be an object", { code: "validation" });
  }
  const kind = approver?.kind ?? "any_human";
  if (kind !== "any_human" && kind !== "user" && kind !== "agent") {
    throw unprocessable("Stage approver kind must be any_human, user, or agent", { code: "validation" });
  }
  const id = typeof approver?.id === "string" ? approver.id.trim() : approver?.id;
  if ((kind === "user" || kind === "agent") && (typeof id !== "string" || id.length === 0)) {
    throw unprocessable("Specific stage approvers require an id", { code: "validation" });
  }
  if (kind === "any_human") {
    return { kind };
  }
  if (!requireApproval) {
    return { kind, id: id as string };
  }
  return { kind, id: id as string };
}

export function assertStageEnabled(stage: typeof pipelineStages.$inferSelect, action: string) {
  const config = normalizeStageConfig(stage.kind, stageConfig(stage));
  if (config.disabled !== true) return;
  throw unprocessable("Pipeline stage is disabled", {
    code: "stage_disabled",
    action,
    stageId: stage.id,
    stageKey: stage.key,
  });
}

export function assertActorCanApproveStageExit(stage: typeof pipelineStages.$inferSelect, actor: PipelineActor) {
  const config = normalizeStageConfig(stage.kind, stageConfig(stage));
  if (config.requireApproval !== true) return;
  const approver = config.approver ?? { kind: "any_human" };
  if (approver.kind === "any_human") {
    if (actor.type === "user") return;
    throw new HttpError(403, "Stage approval requires a human approver", { code: "review_required" });
  }
  if (approver.kind === "user") {
    if (actor.type === "user" && actor.userId === approver.id) return;
    throw new HttpError(403, "Stage approval requires the configured user approver", {
      code: "review_required",
      approver,
    });
  }
  if (actor.type === "agent" && actor.agentId === approver.id) return;
  throw new HttpError(403, "Stage approval requires the configured agent approver", {
    code: "review_required",
    approver,
  });
}

export function targetStageKeyForReviewDecision(config: PipelineStageConfig, decision: PipelineReviewDecision) {
  if (decision === "approve") return config.approveToStageKey!;
  if (decision === "reject") return config.rejectToStageKey!;
  if (!config.requestChangesToStageKey) {
    throw unprocessable("Review stage does not configure requestChangesToStageKey", { code: "validation" });
  }
  return config.requestChangesToStageKey;
}
