import type {} from '@deepseek-ai/dsh-llm'

/** Calendar rule behavior when one or more occurrences elapsed while the Host was unavailable. */
export type MisfirePolicy = 'latest' | 'skip'

/** Version 1 deliberately forbids overlapping executions of the same job. */
export type OverlapPolicy = 'forbid'

/** What caused one occurrence to be admitted. */
export type RunTrigger = 'schedule' | 'manual'

/** Durable lifecycle of one exact occurrence. */
export type RunState =
  | 'claimed'
  | 'enqueued'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'needs_review'
  | 'incomplete'
  | 'timed_out'
  | 'unknown'
  | 'skipped'

export const TERMINAL_RUN_STATES: readonly RunState[] = [
  'succeeded',
  'failed',
  'needs_review',
  'incomplete',
  'timed_out',
  'unknown',
  'skipped',
]

/** One validator reported by the occurrence Agent's terminal tool. */
export interface CronCheck {
  readonly name: string
  readonly passed: boolean
  readonly detail?: string
}

/** Structured completion receipt persisted before the terminal tool returns. */
export interface CronRunReceipt {
  readonly status: 'succeeded' | 'failed' | 'needs_review'
  readonly summary: string
  readonly artifacts: readonly string[]
  readonly checks: readonly CronCheck[]
}

/** Host-owned durable job definition. */
export interface CronJob {
  readonly id: string
  readonly name: string
  readonly expression: string
  readonly timeZone: string
  readonly prompt: string
  readonly cwd: string
  readonly agentPreset: string
  readonly permissionPreset: string
  readonly provider?: string
  readonly model?: string
  readonly allowedTools: readonly string[]
  readonly misfirePolicy: MisfirePolicy
  readonly overlapPolicy: OverlapPolicy
  readonly maxRuntimeMs: number
  readonly enabled: boolean
  readonly nextRunAt?: string
  readonly createdBySessionId: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly revision: number
}

/** Input accepted by the durable store after tool/config validation. */
export interface CreateCronJobInput {
  readonly id: string
  readonly name: string
  readonly expression: string
  readonly timeZone: string
  readonly prompt: string
  readonly cwd: string
  readonly agentPreset: string
  readonly permissionPreset: string
  readonly provider?: string
  readonly model?: string
  readonly allowedTools: readonly string[]
  readonly misfirePolicy: MisfirePolicy
  readonly overlapPolicy: OverlapPolicy
  readonly maxRuntimeMs: number
  readonly enabled: boolean
  readonly nextRunAt?: string
  readonly createdBySessionId: string
  readonly now: string
}

/** Mutable fields supported by cron_update. */
export interface UpdateCronJobInput {
  readonly name?: string
  readonly expression?: string
  readonly timeZone?: string
  readonly prompt?: string
  readonly cwd?: string
  readonly agentPreset?: string
  readonly permissionPreset?: string
  readonly provider?: string | null
  readonly model?: string | null
  readonly misfirePolicy?: MisfirePolicy
  readonly maxRuntimeMs?: number
  readonly enabled?: boolean
  readonly nextRunAt?: string | null
  readonly expectedRevision: number
  readonly now: string
}

/** Host-owned record for one exact scheduled or manual occurrence. */
export interface CronRun {
  readonly id: string
  readonly jobId: string
  readonly scheduledAt: string
  readonly trigger: RunTrigger
  readonly state: RunState
  readonly sessionId: string
  /** Durable id of the Cron-owned user message, once admission reached the Agent. */
  readonly messageId?: string
  readonly attempt: number
  readonly claimedAt: string
  readonly enqueuedAt?: string
  readonly startedAt?: string
  readonly finishedAt?: string
  readonly finishReason?: string
  readonly receipt?: CronRunReceipt
  readonly error?: string
}

/** Durable liveness marker observed by an out-of-process supervisor. */
export interface CronHeartbeat {
  readonly instanceId: string
  readonly pid: number
  readonly startedAt: string
  readonly heartbeatAt: string
}

/** Result of atomically advancing one schedule and claiming its occurrence. */
export interface ClaimResult {
  readonly kind: 'claimed' | 'stale' | 'overlap'
  readonly run?: CronRun
}

/** Narrow store contract used by the runtime and tool layer. */
export interface CronStoreApi {
  listJobs(): readonly CronJob[]
  getJob(id: string): CronJob | undefined
  createJob(input: CreateCronJobInput): CronJob
  updateJob(id: string, input: UpdateCronJobInput): CronJob | undefined
  deleteJob(id: string): boolean
  dueJobs(now: string, limit: number): readonly CronJob[]
  claimScheduled(job: CronJob, scheduledAt: string, nextRunAt: string, now: string, sessionId: string): ClaimResult
  claimManual(job: CronJob, scheduledAt: string, now: string, sessionId: string): ClaimResult
  getRun(id: string): CronRun | undefined
  listRuns(jobId?: string, limit?: number): readonly CronRun[]
  activeRuns(): readonly CronRun[]
  markEnqueued(runId: string, at: string, messageId?: string): CronRun | undefined
  markRunning(runId: string, at: string): CronRun | undefined
  finishRun(runId: string, state: RunState, at: string, details?: {
    readonly finishReason?: string
    readonly receipt?: CronRunReceipt
    readonly error?: string
  }): CronRun | undefined
  writeReceipt(runId: string, receipt: CronRunReceipt, at: string): CronRun | undefined
  writeHeartbeat(heartbeat: CronHeartbeat): void
  readHeartbeat(): CronHeartbeat | undefined
  close(): void
}

/** Durable source attached to each Cron-owned user message. */
export interface CronMessageSource {
  readonly kind: 'cron'
  readonly jobId: string
  readonly runId: string
  readonly scheduledAt: string
}

declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap {
    cron: CronMessageSource
  }
}
