import { and, eq, inArray, isNull, ne } from "drizzle-orm";
import {
  issueComments,
  issues,
  pipelineCaseBlockers,
  pipelineCaseIssueLinks,
  pipelineCases,
} from "@paperclipai/db";
import { visibleIssueCondition } from "./issue-visibility.js";
import { nowDate, writeCaseEvent } from "./pipeline-case-events.js";
import type { PipelineDb } from "./pipeline-case-types.js";

export function buildCaseDeepLink(input: { pipelineId: string; caseId: string }) {
  return `/PAP/pipelines/${input.pipelineId}/cases/${input.caseId}`;
}

export async function notifyDependentWorkIssuesOfUpstreamContentChange(
  db: PipelineDb,
  input: {
    companyId: string;
    upstreamCase: typeof pipelineCases.$inferSelect;
    previousVersion: number;
    version: number;
  },
) {
  const dependents = await db
    .select({ dependentCase: pipelineCases })
    .from(pipelineCaseBlockers)
    .innerJoin(pipelineCases, eq(pipelineCaseBlockers.caseId, pipelineCases.id))
    .where(and(
      eq(pipelineCaseBlockers.companyId, input.companyId),
      eq(pipelineCaseBlockers.blockedByCaseId, input.upstreamCase.id),
      eq(pipelineCases.companyId, input.companyId),
      isNull(pipelineCases.terminalKind),
    ));

  if (dependents.length === 0) return;

  const dependentCaseIds = dependents.map((row) => row.dependentCase.id);
  const linkRows = await db
    .select({ caseId: pipelineCaseIssueLinks.caseId, issueId: issues.id })
    .from(pipelineCaseIssueLinks)
    .innerJoin(issues, eq(pipelineCaseIssueLinks.issueId, issues.id))
    .where(and(
      eq(pipelineCaseIssueLinks.companyId, input.companyId),
      inArray(pipelineCaseIssueLinks.caseId, dependentCaseIds),
      eq(pipelineCaseIssueLinks.role, "work"),
      eq(issues.companyId, input.companyId),
      ne(issues.status, "done"),
      ne(issues.status, "cancelled"),
      visibleIssueCondition(),
    ));
  const issueIdsByCase = new Map<string, string[]>();
  for (const row of linkRows) {
    const list = issueIdsByCase.get(row.caseId) ?? [];
    list.push(row.issueId);
    issueIdsByCase.set(row.caseId, list);
  }

  const upstreamLink = buildCaseDeepLink({
    pipelineId: input.upstreamCase.pipelineId,
    caseId: input.upstreamCase.id,
  });
  const body = `Upstream case [${input.upstreamCase.caseKey}](${upstreamLink}) changed (v${input.previousVersion}→v${input.version}).`;

  const notifiedIssueIds = new Set<string>();
  for (const { dependentCase } of dependents) {
    const issueIds = issueIdsByCase.get(dependentCase.id) ?? [];
    for (const issueId of issueIds) {
      if (notifiedIssueIds.has(issueId)) continue;
      notifiedIssueIds.add(issueId);
      await db.insert(issueComments).values({
        companyId: input.companyId,
        issueId,
        authorType: "system",
        body,
      });
      await db.update(issues).set({ updatedAt: nowDate() }).where(eq(issues.id, issueId));
    }
    // The drift event intentionally does not bump the dependent case's
    // updatedAt: "unresolved drift" is derived as event.createdAt > case.updatedAt.
    await writeCaseEvent(db, {
      companyId: input.companyId,
      caseId: dependentCase.id,
      type: "upstream_drift",
      actor: { type: "system" },
      payload: {
        upstreamCaseId: input.upstreamCase.id,
        upstreamCaseKey: input.upstreamCase.caseKey,
        upstreamPipelineId: input.upstreamCase.pipelineId,
        previousVersion: input.previousVersion,
        version: input.version,
        notifiedIssueIds: issueIds,
      },
    });
  }
}
