import { pipelineAutomationExecutions, pipelineStages } from "@paperclipai/db";
import type { ExecutionWorkspaceMode, IssueExecutionWorkspaceSettings } from "@paperclipai/shared";
import { stageConfig } from "./pipeline-stage-config.js";
import type { PipelineDb, PipelineAutomationExecutionContext } from "./pipeline-case-types.js";

export function readOptionalTrimmedString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

export function readExecutionWorkspacePreference(value: unknown): ExecutionWorkspaceMode | null {
  const preference = readOptionalTrimmedString(value);
  switch (preference) {
    case "inherit":
    case "shared_workspace":
    case "isolated_workspace":
    case "operator_branch":
    case "reuse_existing":
    case "agent_default":
      return preference;
    default:
      return null;
  }
}

export function readExecutionWorkspaceSettings(value: unknown): IssueExecutionWorkspaceSettings | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as IssueExecutionWorkspaceSettings
    : null;
}

export function readAutomationExecutionContext(
  source?: Partial<PipelineAutomationExecutionContext> | null,
): PipelineAutomationExecutionContext {
  return {
    projectId: readOptionalTrimmedString(source?.projectId),
    projectWorkspaceId: readOptionalTrimmedString(source?.projectWorkspaceId),
    executionWorkspaceId: readOptionalTrimmedString(source?.executionWorkspaceId),
    executionWorkspacePreference: readExecutionWorkspacePreference(source?.executionWorkspacePreference),
    executionWorkspaceSettings: readExecutionWorkspaceSettings(source?.executionWorkspaceSettings),
  };
}

export function stageAutomation(stage: typeof pipelineStages.$inferSelect) {
  const onEnter = stageConfig(stage).onEnter;
  if (!onEnter || onEnter.type !== "run_routine" || !onEnter.routineId) return null;
  return {
    id: onEnter.id ?? `${stage.id}:on_enter`,
    routineId: onEnter.routineId,
    ...readAutomationExecutionContext(onEnter),
  };
}

export async function enqueueStageAutomationLedger(
  db: PipelineDb,
  input: {
    companyId: string;
    caseId: string;
    stage: typeof pipelineStages.$inferSelect;
    eventId: string;
    retryOfExecutionId?: string | null;
    generation?: number;
  },
) {
  const automation = stageAutomation(input.stage);
  if (!automation) return null;
  const [ledger] = await db
    .insert(pipelineAutomationExecutions)
    .values({
      companyId: input.companyId,
      caseId: input.caseId,
      automationId: automation.id,
      triggeringEventId: input.eventId,
      routineId: automation.routineId,
      status: "failed",
      retryOfExecutionId: input.retryOfExecutionId ?? null,
      generation: input.generation ?? 1,
      error: "pending_dispatch",
    })
    .onConflictDoNothing({
      target: [
        pipelineAutomationExecutions.caseId,
        pipelineAutomationExecutions.automationId,
        pipelineAutomationExecutions.triggeringEventId,
      ],
    })
    .returning();
  return ledger ?? null;
}
