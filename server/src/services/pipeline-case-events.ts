import { and, eq, inArray, ne } from "drizzle-orm";
import { issueComments, issues, pipelineCaseEvents, pipelineCaseIssueLinks } from "@paperclipai/db";
import { unprocessable } from "../errors.js";
import { visibleIssueCondition } from "./issue-visibility.js";
import type { PipelineActor, PipelineDb } from "./pipeline-case-types.js";
const MAX_FIELDS_BYTES = 64 * 1024;

export function nowDate() {
  return new Date();
}

export function eventActorPatch(actor: PipelineActor) {
  if (actor.type === "agent") {
    assertActorProvenance(actor);
    return { actorType: "agent", actorAgentId: actor.agentId, runId: actor.runId };
  }
  if (actor.type === "user") {
    return { actorType: "user", actorUserId: actor.userId };
  }
  return { actorType: "system" };
}

export function eventActorPayload(actor: PipelineActor) {
  if (actor.type === "agent") return { type: "agent", agentId: actor.agentId, runId: actor.runId };
  if (actor.type === "user") return { type: "user", userId: actor.userId };
  return { type: "system" };
}

export function assertActorProvenance(actor: PipelineActor) {
  if (actor.type === "agent" && !actor.runId) {
    throw unprocessable("Agent pipeline mutations require a run id", { code: "run_id_required" });
  }
}

export function assertJsonSize(value: unknown, label: string) {
  const bytes = Buffer.byteLength(JSON.stringify(value ?? {}), "utf8");
  if (bytes > MAX_FIELDS_BYTES) {
    throw unprocessable(`${label} must be at most 64KB`, { code: "validation" });
  }
}

export function isTerminalKind(kind: string | null | undefined) {
  return kind === "done" || kind === "cancelled";
}

export function terminalKindForStage(kind: string) {
  return isTerminalKind(kind) ? kind : null;
}

export async function writeCaseEvent(
  db: PipelineDb,
  input: {
    companyId: string;
    caseId: string;
    type: string;
    actor: PipelineActor;
    fromStageId?: string | null;
    toStageId?: string | null;
    payload?: Record<string, unknown>;
  },
) {
  const [event] = await db
    .insert(pipelineCaseEvents)
    .values({
      companyId: input.companyId,
      caseId: input.caseId,
      type: input.type,
      ...eventActorPatch(input.actor),
      fromStageId: input.fromStageId ?? null,
      toStageId: input.toStageId ?? null,
      payload: input.payload ?? {},
    })
    .returning();
  return event!;
}

export async function postSystemCommentOnLinkedIssues(
  db: PipelineDb,
  input: {
    companyId: string;
    caseId: string;
    roles: Array<"origin" | "conversation" | "work" | "automation">;
    body: string;
  },
) {
  const rows = await db
    .select({ issueId: issues.id })
    .from(pipelineCaseIssueLinks)
    .innerJoin(issues, eq(pipelineCaseIssueLinks.issueId, issues.id))
    .where(and(
      eq(pipelineCaseIssueLinks.companyId, input.companyId),
      eq(pipelineCaseIssueLinks.caseId, input.caseId),
      inArray(pipelineCaseIssueLinks.role, input.roles),
      ne(issues.status, "done"),
      ne(issues.status, "cancelled"),
      visibleIssueCondition(),
    ));

  for (const row of rows) {
    await db.insert(issueComments).values({
      companyId: input.companyId,
      issueId: row.issueId,
      authorType: "system",
      body: input.body,
    });
    await db.update(issues).set({ updatedAt: nowDate() }).where(eq(issues.id, row.issueId));
  }
}
