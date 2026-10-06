import { and, eq, inArray } from 'drizzle-orm'
import type { Db } from '@paperclipai/db'
import { issueRelations, issues, workspaceOperations } from '@paperclipai/db'

export type IssueDependencyReadiness = {
  issueId: string
  blockerIssueIds: string[]
  unresolvedBlockerIssueIds: string[]
  unresolvedBlockerCount: number
  /** Blockers whose status is `done` but whose execution workspace has not yet finalized. */
  pendingFinalizeBlockerIssueIds: string[]
  allBlockersDone: boolean
  isDependencyReady: boolean
}

export function createIssueDependencyReadiness(issueId: string): IssueDependencyReadiness {
  return {
    issueId,
    blockerIssueIds: [],
    unresolvedBlockerIssueIds: [],
    unresolvedBlockerCount: 0,
    pendingFinalizeBlockerIssueIds: [],
    allBlockersDone: true,
    isDependencyReady: true
  }
}

async function listPendingFinalizeBlockerIssueIds(
  dbOrTx: Pick<Db, 'select'>,
  companyId: string,
  blockerWorkspacePairs: {
    blockerIssueId: string
    executionWorkspaceId: string
  }[]
): Promise<Set<string>> {
  const pending = new Set<string>()
  const blockerIssueIds = [...new Set(blockerWorkspacePairs.map((pair) => pair.blockerIssueId))]
  const executionWorkspaceIds = [
    ...new Set(blockerWorkspacePairs.map((pair) => pair.executionWorkspaceId))
  ]
  if (blockerIssueIds.length === 0 || executionWorkspaceIds.length === 0) {
    return pending
  }
  const blockerWorkspaceKeys = new Set(
    blockerWorkspacePairs.map((pair) => `${pair.blockerIssueId}:${pair.executionWorkspaceId}`)
  )

  const rows = await dbOrTx
    .select({
      issueId: workspaceOperations.issueId,
      executionWorkspaceId: workspaceOperations.executionWorkspaceId,
      phase: workspaceOperations.phase,
      status: workspaceOperations.status,
      startedAt: workspaceOperations.startedAt
    })
    .from(workspaceOperations)
    .where(
      and(
        eq(workspaceOperations.companyId, companyId),
        inArray(workspaceOperations.executionWorkspaceId, executionWorkspaceIds)
      )
    )

  const latestAttributedByBlockerWorkspace = new Map<
    string,
    { phase: string; status: string; startedAt: Date }
  >()
  const latestUnattributedByWorkspace = new Map<
    string,
    { phase: string; status: string; startedAt: Date }
  >()
  const latestSuccessfulFinalizeByWorkspace = new Map<string, Date>()
  for (const row of rows) {
    if (!row.executionWorkspaceId) {
      continue
    }
    if (row.phase === 'workspace_finalize' && row.status === 'succeeded') {
      const current = latestSuccessfulFinalizeByWorkspace.get(row.executionWorkspaceId)
      if (!current || row.startedAt > current) {
        latestSuccessfulFinalizeByWorkspace.set(row.executionWorkspaceId, row.startedAt)
      }
    }
    if (row.issueId) {
      const key = `${row.issueId}:${row.executionWorkspaceId}`
      if (!blockerWorkspaceKeys.has(key)) {
        continue
      }
      const current = latestAttributedByBlockerWorkspace.get(key)
      if (!current || row.startedAt > current.startedAt) {
        latestAttributedByBlockerWorkspace.set(key, {
          phase: row.phase,
          status: row.status,
          startedAt: row.startedAt
        })
      }
      continue
    }

    const current = latestUnattributedByWorkspace.get(row.executionWorkspaceId)
    if (!current || row.startedAt > current.startedAt) {
      latestUnattributedByWorkspace.set(row.executionWorkspaceId, {
        phase: row.phase,
        status: row.status,
        startedAt: row.startedAt
      })
    }
  }

  for (const pair of blockerWorkspacePairs) {
    const latest =
      latestAttributedByBlockerWorkspace.get(
        `${pair.blockerIssueId}:${pair.executionWorkspaceId}`
      ) ?? latestUnattributedByWorkspace.get(pair.executionWorkspaceId)
    if (!latest) {
      continue // no ops recorded -> nothing to finalize for this blocker
    }
    if (latest.phase === 'workspace_finalize' && latest.status === 'succeeded') {
      continue
    }
    const laterSuccessfulFinalize = latestSuccessfulFinalizeByWorkspace.get(
      pair.executionWorkspaceId
    )
    if (laterSuccessfulFinalize && laterSuccessfulFinalize > latest.startedAt) {
      continue
    }
    pending.add(pair.blockerIssueId)
  }

  return pending
}

export async function listIssueDependencyReadinessMap(
  dbOrTx: Pick<Db, 'select'>,
  companyId: string,
  issueIds: string[]
) {
  const uniqueIssueIds = [...new Set(issueIds.filter(Boolean))]
  const readinessMap = new Map<string, IssueDependencyReadiness>()
  for (const issueId of uniqueIssueIds) {
    readinessMap.set(issueId, createIssueDependencyReadiness(issueId))
  }
  if (uniqueIssueIds.length === 0) {
    return readinessMap
  }

  const blockerRows = await dbOrTx
    .select({
      issueId: issueRelations.relatedIssueId,
      blockerIssueId: issueRelations.issueId,
      blockerStatus: issues.status,
      blockerExecutionWorkspaceId: issues.executionWorkspaceId
    })
    .from(issueRelations)
    .innerJoin(issues, eq(issueRelations.issueId, issues.id))
    .where(
      and(
        eq(issueRelations.companyId, companyId),
        eq(issueRelations.type, 'blocks'),
        inArray(issueRelations.relatedIssueId, uniqueIssueIds)
      )
    )

  // Collect issue/workspace pairs of "done" blockers — these are the only ones
  // subject to the workspace-finalize barrier. Blockers that aren't done already
  // mark the dependent as not-ready and don't need a finalize check.
  const doneBlockerWorkspacePairs: {
    blockerIssueId: string
    executionWorkspaceId: string
  }[] = []
  for (const row of blockerRows) {
    if (row.blockerStatus === 'done' && row.blockerExecutionWorkspaceId) {
      doneBlockerWorkspacePairs.push({
        blockerIssueId: row.blockerIssueId,
        executionWorkspaceId: row.blockerExecutionWorkspaceId
      })
    }
  }
  const pendingFinalizeBlockerIssueIds = await listPendingFinalizeBlockerIssueIds(
    dbOrTx,
    companyId,
    doneBlockerWorkspacePairs
  )

  for (const row of blockerRows) {
    const current = readinessMap.get(row.issueId) ?? createIssueDependencyReadiness(row.issueId)
    current.blockerIssueIds.push(row.blockerIssueId)
    // Only done blockers resolve dependents; cancelled blockers stay unresolved
    // until an operator removes or replaces the blocker relationship explicitly.
    if (row.blockerStatus !== 'done') {
      current.unresolvedBlockerIssueIds.push(row.blockerIssueId)
      current.unresolvedBlockerCount += 1
      current.allBlockersDone = false
      current.isDependencyReady = false
    } else if (
      row.blockerExecutionWorkspaceId &&
      pendingFinalizeBlockerIssueIds.has(row.blockerIssueId)
    ) {
      // Workspace-finalize barrier: the blocker's most recent run on its
      // execution workspace hasn't recorded a successful workspace_finalize.
      // Treat the dependent as not-ready until sync-back lands (or the run
      // finalizes); a subsequent finalize wake will re-evaluate readiness.
      // `allBlockersDone` is cleared too so that callers using it as a
      // proxy for "this dependent can proceed" still see the gate.
      current.unresolvedBlockerIssueIds.push(row.blockerIssueId)
      current.unresolvedBlockerCount += 1
      current.pendingFinalizeBlockerIssueIds.push(row.blockerIssueId)
      current.allBlockersDone = false
      current.isDependencyReady = false
    }
    readinessMap.set(row.issueId, current)
  }

  return readinessMap
}
