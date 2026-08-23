/** Execution-time authority checks for Host-level Cron administration tools. */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { HarnessError } from '@deepseek-ai/dsh-llm'
import type { SessionEvent } from '@deepseek-ai/dsh-session'
import type { ToolRunContext } from '@deepseek-ai/dsh-tools'
import { createHash } from 'node:crypto'

type TurnStartEvent = Extract<SessionEvent, { type: 'turn/start' }>

/** Authenticated caller and the exact open turn enclosing one Cron tool call. */
export interface CronAdminExecution {
  readonly agent: Agent
  readonly start: TurnStartEvent
  readonly events: readonly SessionEvent[]
}

/** Runtime credential minted only after a direct-human authority check succeeds. */
export interface CronAdminAuthority {
  /** Exact live root Agent that received the direct human message. */
  readonly agent: Agent
  /** Durable session identity used for creator attribution and audit records. */
  readonly sessionId: string
  /** Cancellation owned by the exact tool invocation. */
  readonly signal: AbortSignal
}

/** Every state-changing model-facing Cron operation. */
export type CronMutationOperation =
  | 'cron_create'
  | 'cron_update'
  | 'cron_pause'
  | 'cron_resume'
  | 'cron_delete'
  | 'cron_run_now'

interface PendingConfirmation {
  /** Turn boundary in which the challenge was first returned to the model. */
  readonly issuedTurnSeq: number
}

const CONFIRMATION_PREFIX = 'DSH_CRON_CONFIRM_v1_'
const MAX_PENDING_CONFIRMATIONS = 64

/**
 * Process-local admission state. The token itself is reproducible from the
 * exact request, while this state proves it was challenged in an earlier turn.
 * Weak agent keys prevent disposed sessions from being retained.
 */
const pendingConfirmations = new WeakMap<Agent, Map<string, PendingConfirmation>>()

/** Throw one stable policy failure through the ordinary tool-error path. */
function reject(message: string, code: string): never {
  throw new HarnessError(message, code)
}

/** Locate the open turn enclosing a model-requested tool call. */
function openTurn(agent: Agent): { start: TurnStartEvent; events: readonly SessionEvent[] } {
  const events = agent.session.events
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const boundary = events[index]
    if (boundary?.type === 'turn/end') {
      return reject('cron administration tools require an open model turn', 'CRON_TOOL_DRIVER_REQUIRED')
    }
    if (boundary?.type === 'turn/start') {
      return { start: boundary, events: events.slice(index + 1) }
    }
  }
  return reject('cron administration tools require an open model turn', 'CRON_TOOL_DRIVER_REQUIRED')
}

/**
 * Authenticate the exact scoped owner, runtime root, and active driver.
 *
 * A scoped registration is the visibility boundary. These checks are the hard
 * execution boundary if a definition is accidentally inherited or invoked by
 * trusted same-process code.
 */
export function cronAdminExecution(
  ctx: Context,
  exec: ToolRunContext,
  owner: Agent,
): CronAdminExecution {
  const agent = exec.agent
  if (agent === undefined) {
    return reject('cron administration tools require a calling agent', 'CRON_TOOL_AGENT_REQUIRED')
  }
  if (agent !== owner) {
    return reject('cron administration tools are scoped to their exact root agent', 'CRON_TOOL_SCOPE_REQUIRED')
  }
  if (ctx.agents.get(agent.id) !== agent
    || agent.status !== 'running'
    || ctx.agents.currentInitiator() !== agent
    || !ctx.agents.roots().includes(agent)) {
    return reject(
      'cron administration tools require the exact live root agent inside its active driver',
      'CRON_TOOL_DRIVER_REQUIRED',
    )
  }
  return { agent, ...openTurn(agent) }
}

/** Direct-human messages in this exact root-agent turn; all other sources stay excluded. */
function directHumanMessages(execution: CronAdminExecution) {
  return execution.events.flatMap(event =>
    event.type === 'user/message' && event.data.source.kind === 'user' ? [event.data] : [])
}

/** Canonical JSON for already-normalized mutation values. */
function canonicalJson(value: unknown): string {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') {
    return JSON.stringify(value)
  }
  if (typeof value === 'number' && Number.isFinite(value)) return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') {
    const record = value as Readonly<Record<string, unknown>>
    const entries = Object.keys(record).sort().map(key =>
      `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    return `{${entries.join(',')}}`
  }
  throw new TypeError('cron confirmation payload must be normalized JSON')
}

/** Deterministic, audit-friendly binding over session, operation, and normalized payload. */
function confirmationToken(
  execution: CronAdminExecution,
  operation: CronMutationOperation,
  normalizedPayload: unknown,
): string {
  const binding = canonicalJson({
    operation,
    payload: normalizedPayload,
    sessionId: String(execution.agent.session.id),
    version: 1,
  })
  return `${CONFIRMATION_PREFIX}${createHash('sha256').update(binding, 'utf8').digest('hex')}`
}

/** Whether the exact marker occurs in text authored by a direct human this turn. */
function humanMessageContains(execution: CronAdminExecution, token: string): boolean {
  return directHumanMessages(execution).some(message =>
    message.content.some(block => block.type === 'text' && block.text.includes(token)))
}

/** Record one bounded pending challenge without refreshing its original turn. */
function issueConfirmation(agent: Agent, token: string, issuedTurnSeq: number): PendingConfirmation {
  let pending = pendingConfirmations.get(agent)
  if (pending === undefined) {
    pending = new Map()
    pendingConfirmations.set(agent, pending)
  }
  const existing = pending.get(token)
  if (existing !== undefined) return existing
  if (pending.size >= MAX_PENDING_CONFIRMATIONS) {
    const oldest = pending.keys().next().value as string | undefined
    if (oldest !== undefined) pending.delete(oldest)
  }
  const challenge = { issuedTurnSeq }
  pending.set(token, challenge)
  return challenge
}

/** Reject with the same machine code and exact marker for every unconfirmed attempt. */
function rejectConfirmation(operation: CronMutationOperation, token: string): never {
  return reject(
    `cron mutation ${operation} requires a second direct-human confirmation. `
      + `In a later top-level user turn, include the exact token ${token} in the human message `
      + `and repeat the same normalized call with confirmation_token set to ${token}`,
    'CRON_TOOL_CONFIRMATION_REQUIRED',
  )
}

/**
 * Mint the only authority credential accepted by Cron mutation APIs.
 *
 * Cron-, plugin-, goal-, and subagent-sourced turns never qualify. Host code
 * that calls `followup()` or `steer()` must therefore attach its non-user
 * source instead of inheriting the default user attestation.
 */
export function requireDirectHumanCronAuthority(
  execution: CronAdminExecution,
  signal: AbortSignal,
  operation: CronMutationOperation,
  normalizedPayload: unknown,
  suppliedToken?: string,
): CronAdminAuthority {
  if (directHumanMessages(execution).length === 0) {
    return reject(
      'this cron operation requires a direct human turn on a top-level agent',
      'CRON_TOOL_AUTHORITY_REQUIRED',
    )
  }

  const expectedToken = confirmationToken(execution, operation, normalizedPayload)
  const pending = issueConfirmation(execution.agent, expectedToken, execution.start.seq)
  if (pending.issuedTurnSeq >= execution.start.seq
    || suppliedToken !== expectedToken
    || !humanMessageContains(execution, expectedToken)) {
    return rejectConfirmation(operation, expectedToken)
  }

  // A confirmation is single-use even if the runtime later rejects a stale
  // revision. Reattempting any write requires a new explicit human turn.
  pendingConfirmations.get(execution.agent)?.delete(expectedToken)
  return {
    agent: execution.agent,
    sessionId: String(execution.agent.session.id),
    signal,
  }
}
