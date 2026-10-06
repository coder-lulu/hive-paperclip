import { and, eq, isNull, or, sql, type SQL } from 'drizzle-orm'
import { issues } from '@paperclipai/db'

/**
 * The original checkout's ownership and execution-lock CAS predicate only.
 * Callers must separately enforce company, status, assignment, pause,
 * dependencies and execution admission, including external stop proof.
 */
export function issueCheckoutOwnershipCondition(
  agentId: string,
  checkoutRunId: string | null
): SQL {
  const sameRunAssigneeCondition = checkoutRunId
    ? and(
        eq(issues.assigneeAgentId, agentId),
        or(isNull(issues.checkoutRunId), eq(issues.checkoutRunId, checkoutRunId))
      )
    : and(eq(issues.assigneeAgentId, agentId), isNull(issues.checkoutRunId))
  const executionLockCondition = checkoutRunId
    ? or(isNull(issues.executionRunId), eq(issues.executionRunId, checkoutRunId))
    : isNull(issues.executionRunId)

  return sql`(${or(isNull(issues.assigneeAgentId), sameRunAssigneeCondition)} and ${executionLockCondition})`
}
