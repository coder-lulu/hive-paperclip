import { pipelineAutomationExecutions, pipelineCases, pipelineStages } from "@paperclipai/db";
import { HttpError } from "../errors.js";
import { childrenGateConfig } from "./pipeline-stage-breakdown.js";
import { assertStageEnabled, stageConfig } from "./pipeline-stage-config.js";
import { getStageByKeyOrThrow } from "./pipeline-case-state.js";
import {
  computeCaseRollup,
  getAncestorCases,
  hasChildrenTerminalEventForRollup,
} from "./pipeline-case-rollups.js";
import {
  postSystemCommentOnLinkedIssues,
  isTerminalKind,
  writeCaseEvent,
} from "./pipeline-case-events.js";
import { transitionCaseMutationInTransaction } from "./pipeline-case-transition.js";
import type { PipelineDb } from "./pipeline-case-types.js";

export async function maybeAutoAdvanceOnStageEntry(
  tx: PipelineDb,
  input: {
    companyId: string;
    caseRow: typeof pipelineCases.$inferSelect;
    stage: typeof pipelineStages.$inferSelect;
    automationLedgers?: Array<typeof pipelineAutomationExecutions.$inferSelect>;
    visitedStageIds?: Set<string>;
  },
) {
  const gate = childrenGateConfig(stageConfig(input.stage));
  const toStageKey = gate.autoAdvanceOnChildrenTerminal;
  if (!toStageKey) return;
  const visited = input.visitedStageIds ?? new Set<string>();
  if (visited.has(input.stage.id)) return;
  const rollup = await computeCaseRollup(tx, input.companyId, input.caseRow.id);
  if (!rollup.complete || (rollup.total === 0 && !gate.explicitZeroChildrenPass)) return;
  const toStage = await getStageByKeyOrThrow(tx, input.caseRow.pipelineId, toStageKey);
  if (toStage.id === input.stage.id) return;
  visited.add(input.stage.id);
  try {
    assertStageEnabled(toStage, "auto_advance");
    await transitionCaseMutationInTransaction(tx, {
      companyId: input.companyId,
      caseId: input.caseRow.id,
      toStageKey,
      expectedVersion: input.caseRow.version,
      actor: { type: "system" },
      transitionClass: "auto",
      reason: "children_terminal",
      automationLedgers: input.automationLedgers,
      autoAdvanceVisitedStageIds: visited,
    });
  } catch (error) {
    // Best-effort: an unsatisfied gate (drift, approval) on the chained
    // advance must not roll back the transition that entered this stage.
    if (!(error instanceof HttpError)) throw error;
  }
}

export async function handleChildrenTerminal(
  tx: PipelineDb,
  companyId: string,
  parentCaseId: string | null | undefined,
  automationLedgers?: Array<typeof pipelineAutomationExecutions.$inferSelect>,
  options: { allowExplicitZeroChildrenPass?: boolean } = {},
) {
  const ancestors = await getAncestorCases(tx, companyId, parentCaseId);
  for (const ancestor of ancestors) {
    const rollup = await computeCaseRollup(tx, companyId, ancestor.case.id);
    const gate = childrenGateConfig(stageConfig(ancestor.stage), {
      explicitZeroChildrenPass: options.allowExplicitZeroChildrenPass,
    });
    if (
      !rollup.complete ||
      (rollup.total === 0 && !gate.explicitZeroChildrenPass) ||
      await hasChildrenTerminalEventForRollup(tx, ancestor.case.id, ancestor.stage.id, rollup)
    ) {
      continue;
    }
    await writeCaseEvent(tx, {
      companyId,
      caseId: ancestor.case.id,
      type: "children_terminal",
      actor: { type: "system" },
      payload: { rollup },
    });
    await postSystemCommentOnLinkedIssues(tx, {
      companyId,
      caseId: ancestor.case.id,
      roles: ["origin", "conversation"],
      body: `All child cases for pipeline case "${ancestor.case.title}" are terminal. Rollup: ${rollup.done} done, ${rollup.cancelled} cancelled, ${rollup.open} open.`,
    });

    const toStageKey = gate.autoAdvanceOnChildrenTerminal;
    if (!toStageKey || isTerminalKind(ancestor.case.terminalKind)) {
      continue;
    }
    try {
      const toStage = await getStageByKeyOrThrow(tx, ancestor.case.pipelineId, toStageKey);
      assertStageEnabled(toStage, "auto_advance");
      if (toStage.id === ancestor.stage.id) continue;
      await transitionCaseMutationInTransaction(tx, {
        companyId,
        caseId: ancestor.case.id,
        toStageKey,
        expectedVersion: ancestor.case.version,
        actor: { type: "system" },
        transitionClass: "auto",
        reason: "children_terminal",
        automationLedgers,
      });
    } catch (error) {
      // Best-effort: an unsatisfied gate (drift, approval, blocker) on the
      // parent advance must not roll back the child transition that triggered it.
      if (!(error instanceof HttpError)) throw error;
    }
  }
}
