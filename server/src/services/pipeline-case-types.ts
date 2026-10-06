import type { Db, pipelineAutomationExecutions } from "@paperclipai/db";
import type {
  EnvBinding,
  ExecutionWorkspaceMode,
  IssueExecutionWorkspaceSettings,
} from "@paperclipai/shared";

export type PipelineActor =
  | { type: "user"; userId: string }
  | { type: "agent"; agentId: string; runId: string }
  | { type: "system" };

export type PipelineStageKind = "open" | "working" | "review" | "done" | "cancelled";

export type PipelineStageConfig = Record<string, unknown> & {
  autonomy?: "manual" | "suggest" | "auto";
  autoAdvanceOnChildrenTerminal?: string;
  approveToStageKey?: string;
  rejectToStageKey?: string;
  requestChangesToStageKey?: string;
  requireRejectReason?: boolean;
  requireRequestChangesReason?: boolean;
  requireChildrenTerminal?: boolean;
  requireNoUnresolvedDrift?: boolean;
  disabled?: boolean;
  requireApproval?: boolean;
  approver?: {
    kind?: "any_human" | "user" | "agent";
    id?: string;
  };
  reviewerKind?: "human" | "any";
  variables?: Array<{
    name?: unknown;
    key?: unknown;
    label?: unknown;
    type?: unknown;
    defaultValue?: unknown;
    options?: unknown;
    required?: unknown;
    showInAddForm?: unknown;
    source?: unknown;
  }>;
  automation?: {
    routineId?: string | null;
    assigneeAgentId?: string | null;
    titleTemplate?: string | null;
    instructionsBody?: string | null;
    projectId?: string | null;
    projectWorkspaceId?: string | null;
    executionWorkspaceId?: string | null;
    executionWorkspacePreference?: ExecutionWorkspaceMode | null;
    executionWorkspaceSettings?: IssueExecutionWorkspaceSettings | null;
    env?: Record<string, EnvBinding> | null;
    latestRoutineRevisionId?: string | null;
    latestRoutineRevisionNumber?: number;
  };
  breakdown?: {
    targetPipelineId?: unknown;
    targetStageKey?: unknown;
    pieceNoun?: unknown;
    carryOverPolicy?: unknown;
    inheritFields?: unknown;
    advanceTo?: unknown;
    waitForPieces?: unknown;
    whenFinishedMoveTo?: unknown;
  };
  onEnter?: {
    type?: "run_routine";
    routineId?: string;
    id?: string;
    projectId?: string | null;
    projectWorkspaceId?: string | null;
    executionWorkspaceId?: string | null;
    executionWorkspacePreference?: ExecutionWorkspaceMode | null;
    executionWorkspaceSettings?: IssueExecutionWorkspaceSettings | null;
  };
};

export type PipelineReviewDecision = "approve" | "reject" | "request_changes";

export type PipelineDb = Db | Parameters<Parameters<Db["transaction"]>[0]>[0];

export interface PipelineBreakdownConfig {
  targetPipelineId: string;
  targetStageKey: string;
  pieceNoun: string;
  carryOverPolicy: PipelineCarryOverPolicy;
  inheritFields: string[];
  advanceTo: string | null;
  waitForPieces: boolean;
  whenFinishedMoveTo: string | null;
}

export interface PipelineCarryOverPolicy {
  version: 1;
  mode: "all_except" | "only";
  includeFields: string[];
  excludeFields: string[];
}

export type PipelineAutomationExecutionContext = {
  projectId: string | null;
  projectWorkspaceId: string | null;
  executionWorkspaceId: string | null;
  executionWorkspacePreference: ExecutionWorkspaceMode | null;
  executionWorkspaceSettings: IssueExecutionWorkspaceSettings | null;
};

// Mirrors the original dispatch accumulator, including the terminal-reparent omission.
export type PipelineCaseTransactionEffects = {
  automationLedgers: Array<typeof pipelineAutomationExecutions.$inferSelect>;
};

export type PipelineCaseReviewInput = {
  companyId: string; caseId: string; decision: PipelineReviewDecision; reason?: string | null;
  edits?: { title?: string; summary?: string | null; fields?: Record<string, unknown>; parentCaseId?: string | null };
  expectedVersion: number; leaseToken?: string | null; actor: PipelineActor;
};
