import { isDeepStrictEqual } from "node:util";
import { and, eq } from "drizzle-orm";
import { pipelineCases } from "@paperclipai/db";
import { conflict } from "../errors.js";
import { assertJsonSize, nowDate, writeCaseEvent, isTerminalKind } from "./pipeline-case-events.js";
import {
  assertLeaseAvailable,
  assertValidParentCase,
  conflictDetailsForCase,
  getCaseWithStageOrThrow,
} from "./pipeline-case-state.js";
import { adjustParentCounts } from "./pipeline-case-rollups.js";
import { handleChildrenTerminal } from "./pipeline-case-children.js";
import { notifyDependentWorkIssuesOfUpstreamContentChange } from "./pipeline-case-drift.js";
import type { PipelineActor, PipelineDb } from "./pipeline-case-types.js";

export async function patchCaseContentInTransaction(
  tx: PipelineDb,
  input: {
    companyId: string;
    caseId: string;
    title?: string;
    summary?: string | null;
    fields?: Record<string, unknown>;
    parentCaseId?: string | null;
    workspaceRef?: Record<string, unknown> | null;
    expectedVersion?: number;
    leaseToken?: string | null;
    actor: PipelineActor;
  },
) {
  if (input.fields !== undefined) assertJsonSize(input.fields, "fields");
  const { case: existing, stage } = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
  const current = await assertLeaseAvailable(tx, existing, input.actor, input.leaseToken);
  if (input.expectedVersion !== undefined && current.version !== input.expectedVersion) {
    throw conflict("Pipeline case version conflict", conflictDetailsForCase(current, stage));
  }
  if (input.parentCaseId !== undefined) {
    await assertValidParentCase(tx, {
      companyId: input.companyId,
      caseId: current.id,
      parentCaseId: input.parentCaseId,
    });
  }
  const titleChanged = input.title !== undefined && input.title !== current.title;
  const summaryChanged = input.summary !== undefined && input.summary !== current.summary;
  const fieldsChanged = input.fields !== undefined && !isDeepStrictEqual(input.fields, current.fields);
  const parentCaseChanged = input.parentCaseId !== undefined && input.parentCaseId !== current.parentCaseId;
  const workspaceRefChanged = input.workspaceRef !== undefined && !isDeepStrictEqual(input.workspaceRef, current.workspaceRef);
  const materialChanged = titleChanged || summaryChanged || fieldsChanged;
  const visibleMetadataChanged = titleChanged || summaryChanged;
  if (!materialChanged && !visibleMetadataChanged && !parentCaseChanged && !workspaceRefChanged) {
    return { case: current, event: null };
  }

  const patch: Partial<typeof pipelineCases.$inferInsert> = {
    updatedAt: nowDate(),
  };
  if (materialChanged) patch.version = current.version + 1;
  if (titleChanged) patch.title = input.title;
  if (summaryChanged) patch.summary = input.summary;
  if (fieldsChanged) patch.fields = input.fields;
  if (parentCaseChanged) patch.parentCaseId = input.parentCaseId;
  if (workspaceRefChanged) patch.workspaceRef = input.workspaceRef;

  const [updated] = await tx
    .update(pipelineCases)
    .set(patch)
    .where(and(eq(pipelineCases.id, current.id), eq(pipelineCases.version, current.version)))
    .returning();
  if (!updated) {
    const latest = await getCaseWithStageOrThrow(tx, input.companyId, input.caseId);
    throw conflict("Pipeline case version conflict", conflictDetailsForCase(latest.case, latest.stage));
  }

  const event = materialChanged || visibleMetadataChanged || parentCaseChanged
    ? await writeCaseEvent(tx, {
      companyId: input.companyId,
      caseId: updated.id,
      type: "updated",
      actor: input.actor,
      payload: {
        previousVersion: current.version,
        version: updated.version,
        parentCaseChanged,
        materialChanged,
        workspaceRefChanged,
      },
    })
    : null;
  if (parentCaseChanged) {
    const terminalDelta = isTerminalKind(current.terminalKind) ? 1 : 0;
    await adjustParentCounts(tx, {
      parentCaseId: current.parentCaseId,
      childDelta: -1,
      terminalChildDelta: -terminalDelta,
    });
    await adjustParentCounts(tx, {
      parentCaseId: input.parentCaseId,
      childDelta: 1,
      terminalChildDelta: terminalDelta,
    });
    if (isTerminalKind(current.terminalKind)) {
      await handleChildrenTerminal(tx, input.companyId, input.parentCaseId);
    }
  }
  if (materialChanged) {
    await notifyDependentWorkIssuesOfUpstreamContentChange(tx, {
      companyId: input.companyId,
      upstreamCase: updated,
      previousVersion: current.version,
      version: updated.version,
    });
  }
  return { case: updated, event };
}
