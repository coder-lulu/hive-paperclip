import { and, asc, eq } from 'drizzle-orm'
import type { Db } from '@paperclipai/db'
import { issues, issueTreeHolds } from '@paperclipai/db'

export type ActiveIssueTreePauseHoldGate = {
  holdId: string
  rootIssueId: string
  issueId: string
  isRoot: boolean
  mode: 'pause'
  reason: string | null
  releasePolicy: typeof issueTreeHolds.$inferSelect.releasePolicy
}

const MAX_PAUSE_HOLD_ANCESTOR_DEPTH = 100

export async function getActivePauseHoldGate(
  dbOrTx: Pick<Db, 'select'>,
  companyId: string,
  issueId: string
): Promise<ActiveIssueTreePauseHoldGate | null> {
  const activePauseHolds = await dbOrTx
    .select({
      id: issueTreeHolds.id,
      rootIssueId: issueTreeHolds.rootIssueId,
      reason: issueTreeHolds.reason,
      releasePolicy: issueTreeHolds.releasePolicy
    })
    .from(issueTreeHolds)
    .where(
      and(
        eq(issueTreeHolds.companyId, companyId),
        eq(issueTreeHolds.status, 'active'),
        eq(issueTreeHolds.mode, 'pause')
      )
    )
    .orderBy(asc(issueTreeHolds.createdAt), asc(issueTreeHolds.id))
  if (activePauseHolds.length === 0) {
    return null
  }

  const holdByRootIssueId = new Map(activePauseHolds.map((hold) => [hold.rootIssueId, hold]))
  let currentIssueId: string | null = issueId
  const visited = new Set<string>()

  while (
    currentIssueId &&
    !visited.has(currentIssueId) &&
    visited.size < MAX_PAUSE_HOLD_ANCESTOR_DEPTH
  ) {
    visited.add(currentIssueId)
    const hold = holdByRootIssueId.get(currentIssueId)
    if (hold) {
      return {
        holdId: hold.id,
        rootIssueId: hold.rootIssueId,
        issueId,
        isRoot: hold.rootIssueId === issueId,
        mode: 'pause',
        reason: hold.reason,
        releasePolicy: hold.releasePolicy
      }
    }

    const parent: { parentId: string | null } | null = await dbOrTx
      .select({ parentId: issues.parentId })
      .from(issues)
      .where(and(eq(issues.id, currentIssueId), eq(issues.companyId, companyId)))
      .then((rows) => rows[0] ?? null)
    currentIssueId = parent?.parentId ?? null
  }

  return null
}
