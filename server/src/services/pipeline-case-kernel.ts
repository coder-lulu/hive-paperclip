import type {
  PipelineCaseReviewInput,
  PipelineCaseTransactionEffects,
  PipelineDb,
} from "./pipeline-case-types.js";
import { getCaseWithStageOrThrow } from "./pipeline-case-state.js";
import { transitionCaseMutationInTransaction } from "./pipeline-case-transition.js";
import { reviewCaseMutationInTransaction } from "./pipeline-case-review.js";

export type PipelineCaseTransitionInput = Omit<
  Parameters<typeof transitionCaseMutationInTransaction>[1],
  "automationLedgers"
>;
export type {
  PipelineActor,
  PipelineCaseReviewInput,
  PipelineCaseTransactionEffects,
  PipelineDb,
} from "./pipeline-case-types.js";
export { getCaseWithStageOrThrow as readPipelineCaseInTransaction } from "./pipeline-case-state.js";

// Callers own transaction and authorization; descriptors never execute a routine here.
export async function transitionCaseInTransaction(tx: PipelineDb, input: PipelineCaseTransitionInput) {
  const effects: PipelineCaseTransactionEffects = { automationLedgers: [] };
  const result = await transitionCaseMutationInTransaction(tx, {
    ...input,
    automationLedgers: effects.automationLedgers,
  });
  const authoritative = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
  return { ...result, authoritativeCase: authoritative.case, effects };
}

export async function reviewCaseInTransaction(tx: PipelineDb, input: PipelineCaseReviewInput) {
  const effects: PipelineCaseTransactionEffects = { automationLedgers: [] };
  const result = await reviewCaseMutationInTransaction(tx, input, effects.automationLedgers);
  const authoritative = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
  return { ...result, authoritativeCase: authoritative.case, effects };
}
