import { unprocessable } from "../errors.js";
import type {
  PipelineStageConfig,
  PipelineCarryOverPolicy,
  PipelineBreakdownConfig,
} from "./pipeline-case-types.js";

export function readOptionalStageKey(value: unknown, label: string) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value !== "string" || value.trim().length === 0) {
    throw unprocessable(`${label} must be a non-empty string`, { code: "validation" });
  }
  return value.trim();
}

export function readStringList(value: unknown, label: string) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw unprocessable(`${label} must be an array`, { code: "validation" });
  const seen = new Set<string>();
  return value.flatMap((entry) => {
    if (typeof entry !== "string" || entry.trim().length === 0) {
      throw unprocessable(`${label} entries must be non-empty strings`, { code: "validation" });
    }
    const key = entry.trim();
    if (seen.has(key)) return [];
    seen.add(key);
    return [key];
  });
}

export function readBreakdownCarryOverPolicy(raw: NonNullable<PipelineStageConfig["breakdown"]>): PipelineCarryOverPolicy {
  const policy = raw.carryOverPolicy;
  if (policy !== undefined && policy !== null) {
    if (!policy || typeof policy !== "object" || Array.isArray(policy)) {
      throw unprocessable("Breakdown carryOverPolicy must be an object", { code: "validation" });
    }
    const record = policy as Record<string, unknown>;
    const version = record.version ?? 1;
    if (version !== 1) {
      throw unprocessable("Breakdown carryOverPolicy version is unsupported", {
        code: "validation",
        version,
      });
    }
    const mode = record.mode ?? "all_except";
    if (mode !== "all_except" && mode !== "only") {
      throw unprocessable("Breakdown carryOverPolicy mode must be all_except or only", { code: "validation" });
    }
    return {
      version: 1,
      mode,
      includeFields: readStringList(record.includeFields, "Breakdown carryOverPolicy includeFields"),
      excludeFields: readStringList(record.excludeFields, "Breakdown carryOverPolicy excludeFields"),
    };
  }
  return {
    version: 1,
    mode: "only",
    includeFields: readStringList(raw.inheritFields, "Breakdown inheritFields"),
    excludeFields: [],
  };
}

export function readBreakdownConfig(config?: PipelineStageConfig | null): PipelineBreakdownConfig | null {
  const raw = config?.breakdown;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const targetPipelineId = typeof raw.targetPipelineId === "string" && raw.targetPipelineId.trim()
    ? raw.targetPipelineId.trim()
    : null;
  const targetStageKey = typeof raw.targetStageKey === "string" && raw.targetStageKey.trim()
    ? raw.targetStageKey.trim()
    : null;
  if (!targetPipelineId) throw unprocessable("Breakdown targetPipelineId is required", { code: "validation" });
  if (!targetStageKey) throw unprocessable("Breakdown targetStageKey is required", { code: "validation" });
  const pieceNoun = typeof raw.pieceNoun === "string" && raw.pieceNoun.trim()
    ? raw.pieceNoun.trim()
    : "piece";
  const waitForPieces = raw.waitForPieces === undefined
    ? config?.requireChildrenTerminal === true
    : raw.waitForPieces === true;
  const whenFinishedMoveTo = readOptionalStageKey(
    raw.whenFinishedMoveTo ?? config?.autoAdvanceOnChildrenTerminal,
    "Breakdown whenFinishedMoveTo",
  );
  const carryOverPolicy = readBreakdownCarryOverPolicy(raw);
  return {
    targetPipelineId,
    targetStageKey,
    pieceNoun,
    carryOverPolicy,
    inheritFields: carryOverPolicy.mode === "only" ? carryOverPolicy.includeFields : [],
    advanceTo: readOptionalStageKey(raw.advanceTo, "Breakdown advanceTo"),
    waitForPieces,
    whenFinishedMoveTo,
  };
}

export function childrenGateConfig(
  config?: PipelineStageConfig | null,
  options: { explicitZeroChildrenPass?: boolean } = {},
) {
  const breakdown = readBreakdownConfig(config);
  return {
    requireChildrenTerminal: breakdown?.waitForPieces ?? config?.requireChildrenTerminal === true,
    autoAdvanceOnChildrenTerminal: breakdown?.whenFinishedMoveTo ?? (
      typeof config?.autoAdvanceOnChildrenTerminal === "string" && config.autoAdvanceOnChildrenTerminal.trim()
        ? config.autoAdvanceOnChildrenTerminal.trim()
        : null
    ),
    explicitZeroChildrenPass: options.explicitZeroChildrenPass === true,
  };
}
