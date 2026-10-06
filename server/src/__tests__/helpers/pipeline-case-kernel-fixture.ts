import { randomUUID } from "node:crypto";
import {
  companies,
  pipelineCases,
  pipelineStages,
  pipelines,
  type Db,
} from "@paperclipai/db";

export async function caseKernelFixture(
  db: Db,
  input: {
    stageKey?: string;
    enforceTransitions?: boolean;
    configs?: Record<string, Record<string, unknown>>;
    casePatch?: Partial<typeof pipelineCases.$inferInsert>;
  } = {},
) {
  const [company] = await db.insert(companies).values({
    name: "Case kernel regression",
    issuePrefix: `K${randomUUID().replace(/-/g, "").slice(0, 6).toUpperCase()}`,
  }).returning();
  if (!company) throw new Error("Missing company fixture");
  const [pipeline] = await db.insert(pipelines).values({
    companyId: company.id,
    key: randomUUID(),
    name: "Case kernel",
    enforceTransitions: input.enforceTransitions ?? false,
  }).returning();
  if (!pipeline) throw new Error("Missing pipeline fixture");
  const stageRows = await db.insert(pipelineStages).values([
    { key: "draft", kind: "working" },
    { key: "review", kind: "review" },
    { key: "produce", kind: "working" },
    { key: "done", kind: "done" },
    { key: "cancelled", kind: "cancelled" },
  ].map((stage, index) => ({
    pipelineId: pipeline.id,
    name: stage.key,
    position: index,
    ...stage,
    config: {
      ...(stage.kind === "review" ? {
        approveToStageKey: "done",
        rejectToStageKey: "cancelled",
        requestChangesToStageKey: "draft",
      } : {}),
      ...input.configs?.[stage.key],
    },
  }))).returning();
  const stages = new Map(stageRows.map((stage) => [stage.key, stage]));
  const stage = stages.get(input.stageKey ?? "draft");
  if (!stage) throw new Error("Missing stage fixture");
  const [caseRow] = await db.insert(pipelineCases).values({
    companyId: company.id,
    pipelineId: pipeline.id,
    stageId: stage.id,
    caseKey: randomUUID(),
    title: "Case fixture",
    ...input.casePatch,
  }).returning();
  if (!caseRow) throw new Error("Missing Case fixture");
  return { company, pipeline, stages, case: caseRow };
}
