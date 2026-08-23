/** Always-on Host scheduler, occurrence admission, recovery, and execution. */

import { randomUUID } from 'node:crypto'
import { isAbsolute } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import {
  foldConsumedWork,
  installModelSelection,
  type Agent,
  type AgentHandle,
  type ModelSelection,
  type ModelSelectionRef,
} from '@deepseek-ai/dsh-agent'
import { HarnessError } from '@deepseek-ai/dsh-llm'
import { SessionId, type SessionEvent } from '@deepseek-ai/dsh-session'
import type {} from '@deepseek-ai/dsh-agent-default-model'
import type {} from '@deepseek-ai/dsh-agent-presets'
import type {} from '@deepseek-ai/dsh-permission-presets'
import type {} from '@deepseek-ai/dsh-session-persistence'
import type {} from '@deepseek-ai/dsh-tools'
import {
  CRON_COMPLETE_TOOL,
  createCronRecoveryWake,
  createCronTaskMessage,
  registerCronCompletionTool,
} from './completion.ts'
import type { ResolvedConfig } from './config.ts'
import {
  deterministicRunId,
  deterministicSessionId,
  latestOccurrence,
  nextOccurrence,
  validateCronExpression,
  validateTimeZone,
} from './cron.ts'
import type { CronAdminAuthority } from './authority.ts'
import type {
  CronAdminCreateInput,
  CronAdminRuntime,
  CronAdminUpdateInput,
  CronRunQuery,
} from './tools.ts'
import {
  TERMINAL_RUN_STATES,
  type CronJob,
  type CronRun,
  type CronStoreApi,
  type RunState,
} from './types.ts'

/** Largest delay Node represents without clamping or warning. */
export const MAX_TIMER_DELAY_MS = 2_147_483_647

const DUE_BATCH_LIMIT = 256
const MIN_RETRY_DELAY_MS = 100
const TERMINAL_RUN_STATE_SET = new Set<RunState>(TERMINAL_RUN_STATES)

type RecoveryMode = 'create' | 'resume-enqueue' | 'resume-pending'

interface QueuedExecution {
  readonly job: CronJob
  readonly run: CronRun
  readonly mode: RecoveryMode
}

interface ActiveExecution {
  readonly entry: QueuedExecution
  readonly abort: AbortController
  promise: Promise<void>
  handle?: AgentHandle
}

interface PersistedRunEvidence {
  readonly exists: boolean
  readonly events?: readonly SessionEvent[]
  readonly cwd?: string
  readonly agentPreset?: string
}

/** Optional deterministic seams used by focused runtime tests. */
export interface CronRuntimeOptions {
  readonly now?: () => number
  readonly instanceId?: string
}

function renderedError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

function cronFailure(message: string, code: string): never {
  throw new HarnessError(message, code)
}

function iso(now: number): string {
  return new Date(now).toISOString()
}

function isTerminal(run: CronRun | undefined): boolean {
  return run !== undefined && TERMINAL_RUN_STATE_SET.has(run.state)
}

function cronMessageForRun(message: { readonly source: { readonly kind: string } }, runId: string): boolean {
  return message.source.kind === 'cron'
    && 'runId' in message.source
    && message.source.runId === runId
}

/** Whether a durable transcript ever accepted this exact occurrence message. */
function hasCronMessage(events: readonly SessionEvent[], runId: string): boolean {
  return events.some((event) => {
    if (event.type === 'user/message') return cronMessageForRun(event.data, runId)
    if (event.type !== 'agent/inbox/spliced') return false
    return event.data.inserted.some(message => cronMessageForRun(message, runId))
  })
}

/** Map a consumed but receipt-less run without ever replaying its side effects. */
function receiptlessTerminal(events: readonly SessionEvent[]): {
  readonly state: Extract<RunState, 'failed' | 'incomplete' | 'unknown'>
  readonly finishReason: string
  readonly error?: string
} | undefined {
  const consumed = foldConsumedWork(events)
  if (consumed.droppedUnrun) {
    return { state: 'incomplete', finishReason: 'inbox_canceled_without_receipt' }
  }
  const reason = consumed.end?.data.reason
  if (reason === undefined) return undefined
  switch (reason.kind) {
    case 'error':
      return {
        state: 'failed',
        finishReason: 'agent_error_without_receipt',
        error: `${reason.error.code}: ${reason.error.message}`,
      }
    case 'aborted':
    case 'interrupted':
      return { state: 'unknown', finishReason: `${reason.kind}_without_receipt` }
    case 'completed':
    case 'blocked':
    case 'max-tokens':
      return { state: 'incomplete', finishReason: `${reason.kind}_without_cron_complete` }
    default:
      return { state: 'unknown', finishReason: 'unknown_turn_end_without_receipt' }
  }
}

/** Host-owned durable scheduler. Timers are only projections of stored nextRunAt. */
export class CronRuntime implements CronAdminRuntime {
  private readonly now: () => number
  private readonly instanceId: string
  private readonly startedAt: string
  private readonly stop = Promise.withResolvers<void>()
  private readonly runnerAgents = new WeakSet<Agent>()
  private readonly queue: QueuedExecution[] = []
  private readonly active = new Map<string, ActiveExecution>()

  private timer: ReturnType<typeof setTimeout> | undefined
  private heartbeat: ReturnType<typeof setInterval> | undefined
  private startup: Promise<void> | undefined
  private pump: Promise<void> | undefined
  private requested = false
  private bootstrapped = false
  private stopping = false
  private fault: unknown
  private disposal: Promise<void> | undefined

  constructor(
    private readonly ctx: Context,
    private readonly store: CronStoreApi,
    private readonly config: ResolvedConfig,
    options: CronRuntimeOptions = {},
  ) {
    this.now = options.now ?? Date.now
    this.instanceId = options.instanceId ?? randomUUID()
    this.startedAt = iso(this.now())
  }

  /** Begin readiness wait, heartbeat, recovery, and the first durable projection. */
  start(): Promise<void> {
    if (this.startup !== undefined) return this.startup
    let task: Promise<void>
    try {
      task = this.ctx.agents.withoutInitiator(() => this.bootstrap())
    } catch (error: unknown) {
      this.tripFault(error)
      task = Promise.resolve()
    }
    this.startup = task.catch((error: unknown) => {
      this.tripFault(error)
      if (!this.stopping) this.ctx.logger.warn(`cron: startup failed: ${renderedError(error)}`)
    })
    return this.startup
  }

  /** True only for a runner marked during unpublished Agent setup. */
  isRunnerAgent(agent: Agent): boolean {
    return this.runnerAgents.has(agent)
  }

  /** Stop new work, cooperatively cancel every Agent, and await full quiescence. */
  dispose(): Promise<void> {
    return (this.disposal ??= (async () => {
      this.stopping = true
      this.requested = false
      this.clearTimer()
      this.clearHeartbeat()
      this.stop.resolve()

      for (const execution of this.active.values()) {
        execution.abort.abort(new Error('Cron runtime is stopping'))
        if (execution.handle !== undefined) {
          void execution.handle.dispose().catch((error: unknown) => {
            this.ctx.logger.warn(`cron: runner disposal failed: ${renderedError(error)}`)
          })
        }
      }

      await Promise.allSettled([this.startup, this.pump].filter((value): value is Promise<void> => value !== undefined))
      while (this.active.size > 0) {
        await Promise.allSettled([...this.active.values()].map(execution => execution.promise))
      }
    })())
  }

  /** Complete boot only after sibling Loader rows have settled. */
  private async bootstrap(): Promise<void> {
    const loader = this.ctx.get('loader') as { await(): Promise<unknown> } | undefined
    if (loader !== undefined) {
      await Promise.race([loader.await(), this.stop.promise])
    }
    if (this.stopping) return

    this.writeHeartbeat()
    this.heartbeat = setInterval(() => {
      if (this.stopping || this.fault !== undefined) return
      try {
        this.writeHeartbeat()
      } catch (error: unknown) {
        this.tripFault(error)
        this.ctx.logger.warn(`cron: heartbeat failed; scheduling stopped: ${renderedError(error)}`)
      }
    }, this.config.heartbeatIntervalMs)
    this.heartbeat.unref?.()

    await this.reconcile()
    if (this.stopping) return
    this.bootstrapped = true
    this.requestDrive()
  }

  private writeHeartbeat(): void {
    const heartbeatAt = Math.max(this.now(), Date.parse(this.startedAt))
    this.store.writeHeartbeat({
      instanceId: this.instanceId,
      pid: process.pid,
      startedAt: this.startedAt,
      heartbeatAt: iso(heartbeatAt),
    })
  }

  /** Inspect every active store row before admitting any new scheduled work. */
  private async reconcile(): Promise<void> {
    const snapshots = await this.ctx.sessionPersistence.listSnapshots()
    const persisted = new Set(snapshots.map(snapshot => String(snapshot.header.id)))

    for (const run of this.store.activeRuns()) {
      if (this.stopping) return
      const job = this.store.getJob(run.jobId)
      if (job === undefined) {
        this.requireFinished(run.id, 'unknown', 'recovery_job_missing')
        continue
      }

      let evidence: PersistedRunEvidence = { exists: false }
      if (persisted.has(run.sessionId)) {
        const inspection = await this.ctx.sessionPersistence.inspect(SessionId(run.sessionId))
        evidence = {
          exists: true,
          events: inspection.events,
          ...inspection.meta.cwd === undefined ? {} : { cwd: inspection.meta.cwd },
          ...inspection.meta.agentPreset === undefined ? {} : { agentPreset: inspection.meta.agentPreset },
        }
      }
      this.reconcileRun(job, run, evidence)
    }
  }

  private reconcileRun(job: CronJob, run: CronRun, evidence: PersistedRunEvidence): void {
    if (!evidence.exists) {
      if (run.state === 'claimed') {
        this.enqueue({ job, run, mode: 'create' })
      } else {
        this.requireFinished(run.id, 'unknown', 'recovery_session_missing')
      }
      return
    }

    const events = evidence.events ?? []
    if (evidence.cwd !== job.cwd || evidence.agentPreset !== job.agentPreset) {
      this.requireFinished(run.id, 'unknown', 'recovery_session_configuration_mismatch')
      return
    }
    const terminal = receiptlessTerminal(events)
    if (terminal !== undefined) {
      this.requireFinished(run.id, terminal.state, terminal.finishReason, terminal.error)
      return
    }

    const hasMessage = hasCronMessage(events, run.id)
    if (!hasMessage && run.state !== 'claimed') {
      this.requireFinished(run.id, 'unknown', 'recovery_admission_record_without_message')
      return
    }
    this.enqueue({ job, run, mode: hasMessage ? 'resume-pending' : 'resume-enqueue' })
  }

  private requireFinished(
    runId: string,
    state: Extract<RunState, 'failed' | 'incomplete' | 'unknown'>,
    finishReason: string,
    error?: string,
  ): void {
    const finished = this.store.finishRun(runId, state, iso(this.now()), {
      finishReason,
      ...error === undefined ? {} : { error },
    })
    if (finished === undefined) throw new Error(`cron run ${runId} disappeared during reconciliation`)
  }

  private enqueue(entry: QueuedExecution): void {
    if (this.queue.some(candidate => candidate.run.id === entry.run.id)
      || this.active.has(entry.run.id)) return
    this.queue.push(entry)
  }

  /** Coalesce timer, admin, and completion triggers into one host-level pump. */
  requestDrive(): void {
    if (this.stopping || this.fault !== undefined) return
    this.clearTimer()
    this.requested = true
    if (!this.bootstrapped || this.pump !== undefined) return

    let task: Promise<void>
    try {
      task = this.ctx.agents.withoutInitiator(() => this.runRequested())
    } catch (error: unknown) {
      this.tripFault(error)
      this.ctx.logger.warn(`cron: could not start scheduler pump: ${renderedError(error)}`)
      return
    }
    this.pump = task
    void task.then(
      () => { this.retirePump(task) },
      (error: unknown) => {
        this.tripFault(error)
        this.ctx.logger.warn(`cron: scheduler pump failed: ${renderedError(error)}`)
        this.retirePump(task)
      },
    )
  }

  private retirePump(task: Promise<void>): void {
    if (this.pump !== task) return
    this.pump = undefined
    if (this.requested && !this.stopping && this.fault === undefined) this.requestDrive()
  }

  private async runRequested(): Promise<void> {
    while (this.requested && !this.stopping && this.fault === undefined) {
      this.requested = false
      this.driveOnce()
      // Let execution setup and completion callbacks publish state before a
      // coalesced synchronous admin trigger asks for another projection.
      await Promise.resolve()
    }
  }

  private driveOnce(): void {
    this.clearTimer()
    this.launchQueued()

    const now = iso(this.now())
    // Claim due rows even when every execution slot is occupied. A claimed or
    // queued occurrence is an active run in SQLite, so a later occurrence for
    // that same Job is atomically advanced and recorded as overlap_forbid.
    // Deferring claims until global capacity returned would instead execute
    // those occurrences late and could create an endless catch-up chain.
    const due = this.store.dueJobs(now, DUE_BATCH_LIMIT)
    for (const job of due) {
      const first = job.nextRunAt
      if (first === undefined) continue
      const scheduledAt = latestOccurrence(job.expression, job.timeZone, first, now)
      if (scheduledAt === undefined) continue
      const nextRunAt = nextOccurrence(job.expression, job.timeZone, scheduledAt)
      const sessionId = deterministicSessionId(job.id, scheduledAt, 'schedule')
      const claimed = this.store.claimScheduled(job, scheduledAt, nextRunAt, now, sessionId)
      if (claimed.kind === 'stale') continue
      if (claimed.kind === 'overlap') continue
      if (claimed.run === undefined) throw new Error(`cron job ${job.id} returned a claim without a run`)

      // `startedAt` is a stable recovery cutoff, not a one-pump flag: when
      // capacity is one, startup due rows may wait behind many earlier rows.
      // Every occurrence already due at Host start remains a startup misfire
      // no matter which later pump finally claims it.
      const isMisfire = first <= this.startedAt || scheduledAt !== first
      if (job.misfirePolicy === 'skip' && isMisfire) {
        const skipped = this.store.finishRun(claimed.run.id, 'skipped', now, {
          finishReason: 'misfire_skip',
        })
        if (skipped === undefined) throw new Error(`cron run ${claimed.run.id} disappeared while skipping misfire`)
      } else {
        this.enqueue({ job, run: claimed.run, mode: 'create' })
      }
    }
    this.launchQueued()
    // The timer stays armed while work is active or queued so a long-running
    // Job cannot hide its own next occurrence until it finishes.
    this.armFromStore()
  }

  private launchQueued(): void {
    while (!this.stopping
      && this.active.size < this.config.maxConcurrentRuns
      && this.queue.length > 0) {
      const entry = this.queue.shift()
      if (entry === undefined) return
      const current = this.store.getRun(entry.run.id)
      if (current === undefined || isTerminal(current)) continue

      const abort = new AbortController()
      const active: ActiveExecution = {
        entry: { ...entry, run: current },
        abort,
        promise: Promise.resolve(),
      }
      this.active.set(current.id, active)
      let promise: Promise<void>
      try {
        promise = this.ctx.agents.withoutInitiator(() => this.execute(active))
      } catch (error: unknown) {
        promise = Promise.reject(error)
      }
      active.promise = promise.catch((error: unknown) => {
        this.ctx.logger.warn(`cron: run ${current.id} escaped containment: ${renderedError(error)}`)
        this.finishSafely(current.id, 'unknown', 'runtime_unhandled_error', renderedError(error))
      }).finally(() => {
        this.active.delete(current.id)
        if (!this.stopping) this.requestDrive()
      })
    }
  }

  /** Rebuild the one next wake from durable job rows; no timer is authoritative. */
  private armFromStore(): void {
    const next = this.store.listJobs()
      .filter(job => job.enabled && job.nextRunAt !== undefined)
      .map(job => Date.parse(job.nextRunAt as string))
      .filter(Number.isFinite)
      .sort((left, right) => left - right)[0]
    if (next === undefined) return
    const delay = Math.max(
      next <= this.now() ? MIN_RETRY_DELAY_MS : next - this.now(),
      0,
    )
    this.timer = setTimeout(() => {
      this.timer = undefined
      this.requestDrive()
    }, Math.min(delay, MAX_TIMER_DELAY_MS))
    this.timer.unref?.()
  }

  private clearTimer(): void {
    if (this.timer === undefined) return
    clearTimeout(this.timer)
    this.timer = undefined
  }

  private clearHeartbeat(): void {
    if (this.heartbeat === undefined) return
    clearInterval(this.heartbeat)
    this.heartbeat = undefined
  }

  /** A faulted scheduler must never continue advertising itself as healthy. */
  private tripFault(error: unknown): void {
    this.fault ??= error
    this.requested = false
    this.clearTimer()
    this.clearHeartbeat()
  }

  /** Keep a failed two-store admission from waking when maintenance releases. */
  private async rollbackAdmission(agent: Agent, reason: string): Promise<void> {
    // These are fresh, occurrence-exclusive Sessions, so clearing the inbox
    // cannot discard work belonging to a human or another job.
    agent.cancel({ kind: 'hook', reason })
    try {
      if (!await this.ctx.sessions.flush(agent.session)) {
        this.ctx.logger.warn(`cron: admission rollback for ${agent.id} was not durable`)
      }
    } catch (error: unknown) {
      this.ctx.logger.warn(`cron: admission rollback flush failed for ${agent.id}: ${renderedError(error)}`)
    }
  }

  /** Commit the running ledger state before maintenance can release its wake latch. */
  private requireRunning(runId: string): void {
    const running = this.store.markRunning(runId, iso(this.now()))
    if (running === undefined) {
      throw new Error('failed to mark the Cron occurrence running')
    }
  }

  /** Create/resume, durably admit, wait, classify, flush, and dispose one occurrence. */
  private async execute(execution: ActiveExecution): Promise<void> {
    const { job, run, mode } = execution.entry
    let admitted = mode === 'resume-pending'
    try {
      const selected = this.selectionFor(job)
      this.ctx.permissionPresets.resolve(job.permissionPreset)
      const preset = await this.ctx.agentPresets.resolve(job.agentPreset)
      execution.abort.signal.throwIfAborted()

      let completion: ReturnType<typeof registerCronCompletionTool> | undefined
      const setup = async (agentCtx: Context): Promise<void> => {
        const agent = agentCtx.agent
        if (agent === undefined) throw new Error('Cron runner setup has no scoped Agent')
        this.runnerAgents.add(agent)
        const selection: ModelSelectionRef = { current: selected, assembled: undefined }
        installModelSelection(agentCtx, selection)
        await this.ctx.agentPresets.mount(agentCtx, preset.id)

        completion = registerCronCompletionTool(
          this.ctx,
          agentCtx,
          agent,
          run.id,
          this.store,
          () => iso(this.now()),
        )

        const deploymentAllowed = new Set(this.config.allowedTools)
        const outsideDeploymentPolicy = job.allowedTools
          .filter(name => !deploymentAllowed.has(name))
        if (outsideDeploymentPolicy.length > 0) {
          throw new HarnessError(
            `Cron job ${job.id} names tools outside the current deployment allowlist: ${outsideDeploymentPolicy.join(', ')}`,
            'CRON_TOOL_POLICY_DRIFT',
          )
        }
        // Do not silently intersect or probe the registry. restrict() validates
        // every durable name and fails setup on stale/misspelled capabilities.
        const allowed = new Set(job.allowedTools)
        agentCtx.tools.restrict({ allow: [...allowed] })
        agentCtx.tools.guard((toolExecution) => {
          if (toolExecution.agent !== agent) return 'Cron tool execution is bound to a different Agent'
          if (toolExecution.name === CRON_COMPLETE_TOOL || toolExecution.name === 'run_code') return undefined
          return allowed.has(toolExecution.name)
            ? undefined
            : `Tool ${toolExecution.name} is outside this Cron job's durable allowlist`
        })
      }

      const handle = mode === 'create'
        ? await this.ctx.agents.create({
            sessionId: SessionId(run.sessionId),
            agentOptions: { provider: selected.provider, model: selected.model },
            meta: { cwd: job.cwd, agentPreset: preset.id },
            signal: execution.abort.signal,
            setup,
          })
        : await this.ctx.agents.resume({
            resumeSessionId: SessionId(run.sessionId),
            agentOptions: { provider: selected.provider, model: selected.model },
            signal: execution.abort.signal,
            setup,
          })
      execution.handle = handle
      const { agent } = handle
      await agent.whenIdle()
      execution.abort.signal.throwIfAborted()

      if (mode === 'resume-pending') {
        const pending = [...agent.inbox.nextStep, ...agent.inbox.nextTurn]
          .find(message => cronMessageForRun(message, run.id))
        if (pending === undefined) {
          throw new Error('durable Cron message is no longer pending after resume')
        }
        if (this.ctx.permissionPresets.current(agent.session.events) !== job.permissionPreset) {
          throw new Error('recovered Cron permission preset differs from the durable job')
        }
        await agent.runMaintenance(async () => {
          const wake = createCronRecoveryWake(job, run)
          // Inbox.claim() consumes every next-step message before the pending
          // next-turn task. A steer therefore wakes the existing occurrence in
          // the same LLM step; a second followup would create an extra turn.
          agent.steer(wake)
          try {
            if (!await this.ctx.sessions.flush(agent.session)) {
              throw new Error('Cron recovery requires configured session persistence')
            }
            const current = this.store.getRun(run.id)
            if (current?.state === 'claimed' || current?.state === 'enqueued') {
              const enqueued = this.store.markEnqueued(run.id, iso(this.now()), String(pending.id))
              if (enqueued === undefined) {
                throw new Error('failed to reconcile the durable Cron admission record')
              }
            } else if (current?.messageId !== undefined && current.messageId !== String(pending.id)) {
              throw new Error('recovered Cron admission names a different durable message')
            }
            this.requireRunning(run.id)
          } catch (error: unknown) {
            await this.rollbackAdmission(agent, 'cron recovery admission rollback')
            throw error
          }
        })
      } else {
        await agent.runMaintenance(async () => {
          this.ctx.permissionPresets.set(agent.session, job.permissionPreset)
          const message = createCronTaskMessage(job, run)
          agent.followup(message)
          try {
            if (!await this.ctx.sessions.flush(agent.session)) {
              throw new Error('Cron admission requires configured session persistence')
            }
            const enqueued = this.store.markEnqueued(run.id, iso(this.now()), String(message.id))
            if (enqueued === undefined) throw new Error('failed to persist the Cron admission record')
            this.requireRunning(run.id)
          } catch (error: unknown) {
            await this.rollbackAdmission(agent, 'cron admission rollback')
            throw error
          }
        })
        admitted = true
      }

      const settled = await this.waitForRun(agent, run.id, job.maxRuntimeMs)
      if (settled === 'timed_out') {
        this.finishSafely(run.id, 'timed_out', 'max_runtime_exceeded')
        return
      }
      if (settled === 'stopped') {
        this.finishSafely(run.id, 'unknown', 'runtime_disposed')
        return
      }

      let durable = false
      try {
        durable = await this.ctx.sessions.flush(agent.session)
      } catch (error: unknown) {
        if (isTerminal(this.store.getRun(run.id))) {
          this.ctx.logger.warn(`cron: transcript flush failed after terminal receipt for ${run.id}: ${renderedError(error)}`)
          return
        }
        throw error
      }
      if (!durable && !isTerminal(this.store.getRun(run.id))) {
        this.finishSafely(run.id, 'unknown', 'terminal_transcript_not_durable')
        return
      }
      if (completion?.captured() !== undefined || isTerminal(this.store.getRun(run.id))) return

      const terminal = receiptlessTerminal(agent.session.events)
      if (terminal === undefined) {
        this.finishSafely(run.id, 'incomplete', 'agent_idle_without_consumed_work')
      } else {
        this.finishSafely(run.id, terminal.state, terminal.finishReason, terminal.error)
      }
    } catch (error: unknown) {
      if (this.stopping || execution.abort.signal.aborted) {
        this.finishSafely(run.id, 'unknown', 'runtime_disposed', renderedError(error))
      } else {
        this.finishSafely(
          run.id,
          admitted ? 'unknown' : 'failed',
          admitted ? 'execution_failed_after_admission' : 'runner_setup_or_admission_failed',
          renderedError(error),
        )
      }
    } finally {
      if (execution.handle !== undefined) {
        try {
          await execution.handle.dispose()
        } catch (error: unknown) {
          this.ctx.logger.warn(`cron: runner ${run.sessionId} did not dispose cleanly: ${renderedError(error)}`)
        }
      }
    }
  }

  private selectionFor(job: CronJob): ModelSelection {
    const fallback = this.ctx.agentDefaultModel.currentSelection()
    const routeOverridden = job.provider !== undefined || job.model !== undefined
    return {
      provider: job.provider ?? fallback.provider,
      model: job.model ?? fallback.model,
      ...routeOverridden || fallback.reasoningEffort === undefined
        ? {}
        : { reasoningEffort: fallback.reasoningEffort },
    }
  }

  /** Wait for quiescence without abandoning the Agent on timeout. */
  private async waitForRun(
    agent: Agent,
    runId: string,
    maxRuntimeMs: number,
  ): Promise<'idle' | 'timed_out' | 'stopped'> {
    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<'timed_out'>(resolve => {
      timer = setTimeout(() => { resolve('timed_out') }, maxRuntimeMs)
      timer.unref?.()
    })
    const outcome = await Promise.race([
      agent.whenIdle().then(() => 'idle' as const),
      timeout,
      this.stop.promise.then(() => 'stopped' as const),
    ])
    if (timer !== undefined) clearTimeout(timer)
    if (outcome === 'idle') return outcome

    if (!isTerminal(this.store.getRun(runId))) {
      agent.cancel({ kind: 'hook', reason: this.stopping ? 'cron runtime stopping' : 'cron timeout' })
    }
    await agent.whenIdle()
    try {
      await this.ctx.sessions.flush(agent.session)
    } catch (error: unknown) {
      this.ctx.logger.warn(`cron: timeout transcript flush failed for ${agent.id}: ${renderedError(error)}`)
    }
    return outcome
  }

  private finishSafely(
    runId: string,
    state: Extract<RunState, 'failed' | 'incomplete' | 'timed_out' | 'unknown'>,
    finishReason: string,
    error?: string,
  ): void {
    try {
      const current = this.store.getRun(runId)
      if (current === undefined || isTerminal(current)) return
      const finished = this.store.finishRun(runId, state, iso(this.now()), {
        finishReason,
        ...error === undefined ? {} : { error },
      })
      if (finished === undefined) throw new Error(`cron run ${runId} disappeared while finishing`)
    } catch (finishError: unknown) {
      this.tripFault(finishError)
      this.ctx.logger.warn(`cron: could not finish run ${runId}: ${renderedError(finishError)}`)
    }
  }

  private async awaitStartup(): Promise<void> {
    await (this.startup ?? this.start())
  }

  private assertOperational(): void {
    if (this.stopping) cronFailure('Cron runtime is stopping', 'CRON_RUNTIME_STOPPING')
    if (this.fault !== undefined) {
      cronFailure(`Cron runtime is faulted: ${renderedError(this.fault)}`, 'CRON_RUNTIME_FAULTED')
    }
  }

  private assertAuthority(authority: CronAdminAuthority): void {
    authority.signal.throwIfAborted()
    if (this.ctx.agents.get(authority.agent.id) !== authority.agent
      || !this.ctx.agents.roots().includes(authority.agent)
      || this.ctx.agents.currentInitiator() !== authority.agent
      || String(authority.agent.session.id) !== authority.sessionId) {
      cronFailure('Cron mutation authority is no longer live', 'CRON_AUTHORITY_EXPIRED')
    }
  }

  private currentJob(id: string, expectedRevision?: number): CronJob {
    const job = this.store.getJob(id)
    if (job === undefined) cronFailure(`Cron job ${id} does not exist`, 'CRON_JOB_NOT_FOUND')
    if (expectedRevision !== undefined && job.revision !== expectedRevision) {
      cronFailure(`Cron job ${id} changed; read its current revision and retry`, 'CRON_REVISION_CONFLICT')
    }
    return job
  }

  private hasActiveRun(jobId: string): boolean {
    return this.store.activeRuns().some(run => run.jobId === jobId)
  }

  private canonicalTimeZone(value: string): string {
    let canonical: string
    try {
      canonical = new Intl.DateTimeFormat('en-US', { timeZone: value }).resolvedOptions().timeZone
      validateTimeZone(canonical)
    } catch (error: unknown) {
      throw new HarnessError(`invalid Cron time zone ${JSON.stringify(value)}`, 'CRON_TIME_ZONE_INVALID', { cause: error })
    }
    return canonical
  }

  private async validateComposition(agentPreset: string, permissionPreset: string): Promise<string> {
    this.ctx.permissionPresets.resolve(permissionPreset)
    return (await this.ctx.agentPresets.resolve(agentPreset)).id
  }

  async listJobs(): Promise<readonly CronJob[]> {
    await this.awaitStartup()
    return this.store.listJobs()
  }

  async createJob(input: CronAdminCreateInput, authority: CronAdminAuthority): Promise<CronJob> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    const now = iso(this.now())
    const timeZone = this.canonicalTimeZone(input.timeZone ?? this.config.defaultTimeZone)
    validateCronExpression(input.expression)
    const cwd = input.cwd ?? this.config.defaultCwd ?? authority.agent.session.header.cwd
    if (cwd === undefined || !isAbsolute(cwd)) {
      cronFailure('Cron job requires an absolute cwd or configured defaultCwd', 'CRON_CWD_REQUIRED')
    }
    const agentPreset = input.agentPreset ?? this.config.defaultAgentPreset
    const permissionPreset = input.permissionPreset ?? this.config.defaultPermissionPreset
    const resolvedPreset = await this.validateComposition(agentPreset, permissionPreset)
    this.assertAuthority(authority)
    const created = this.store.createJob({
      id: `cron-job-${randomUUID()}`,
      name: input.name,
      expression: input.expression,
      timeZone,
      prompt: input.prompt,
      cwd,
      agentPreset: resolvedPreset,
      permissionPreset,
      ...input.provider === undefined ? {} : { provider: input.provider },
      ...input.model === undefined ? {} : { model: input.model },
      allowedTools: [...this.config.allowedTools],
      misfirePolicy: input.misfirePolicy ?? this.config.defaultMisfirePolicy,
      overlapPolicy: 'forbid',
      maxRuntimeMs: input.maxRuntimeMs ?? this.config.maxRuntimeMs,
      enabled: true,
      nextRunAt: nextOccurrence(input.expression, timeZone, now),
      createdBySessionId: authority.sessionId,
      now,
    })
    this.requestDrive()
    return created
  }

  async updateJob(id: string, input: CronAdminUpdateInput, authority: CronAdminAuthority): Promise<CronJob> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    const current = this.currentJob(id, input.expectedRevision)
    if (this.hasActiveRun(id)) {
      cronFailure('Cron job cannot change while an occurrence is active', 'CRON_JOB_ACTIVE')
    }
    const expression = input.expression ?? current.expression
    const timeZone = this.canonicalTimeZone(input.timeZone ?? current.timeZone)
    validateCronExpression(expression)
    const cwd = input.cwd ?? current.cwd
    if (!isAbsolute(cwd)) cronFailure('Cron cwd must be absolute', 'CRON_CWD_INVALID')
    const agentPreset = input.agentPreset ?? current.agentPreset
    const permissionPreset = input.permissionPreset ?? current.permissionPreset
    const resolvedPreset = await this.validateComposition(agentPreset, permissionPreset)
    this.assertAuthority(authority)
    // validateComposition crosses an async boundary. A manual occurrence can
    // be claimed without changing the Job revision while that resolution is
    // pending, so re-check immediately before the synchronous revision-CAS.
    if (this.hasActiveRun(id)) {
      cronFailure('Cron job cannot change while an occurrence is active', 'CRON_JOB_ACTIVE')
    }
    const scheduleChanged = input.expression !== undefined || input.timeZone !== undefined
    const updated = this.store.updateJob(id, {
      ...input,
      expression,
      timeZone,
      cwd,
      agentPreset: resolvedPreset,
      permissionPreset,
      ...scheduleChanged && current.enabled
        ? { nextRunAt: nextOccurrence(expression, timeZone, iso(this.now())) }
        : {},
      now: iso(this.now()),
    })
    if (updated === undefined) {
      cronFailure(`Cron job ${id} changed; read its current revision and retry`, 'CRON_REVISION_CONFLICT')
    }
    this.requestDrive()
    return updated
  }

  async pauseJob(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronJob> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    this.currentJob(id, expectedRevision)
    const updated = this.store.updateJob(id, {
      enabled: false,
      nextRunAt: null,
      expectedRevision,
      now: iso(this.now()),
    })
    if (updated === undefined) cronFailure('Cron revision changed while pausing', 'CRON_REVISION_CONFLICT')
    this.requestDrive()
    return updated
  }

  async resumeJob(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronJob> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    const current = this.currentJob(id, expectedRevision)
    // Resume is idempotent. Recomputing nextRunAt for an already-enabled Job
    // could silently discard an occurrence that is currently due.
    if (current.enabled) return current
    const updated = this.store.updateJob(id, {
      enabled: true,
      nextRunAt: nextOccurrence(current.expression, current.timeZone, iso(this.now())),
      expectedRevision,
      now: iso(this.now()),
    })
    if (updated === undefined) cronFailure('Cron revision changed while resuming', 'CRON_REVISION_CONFLICT')
    this.requestDrive()
    return updated
  }

  async deleteJob(
    id: string,
    expectedRevision: number,
    authority: CronAdminAuthority,
  ): Promise<{ readonly id: string; readonly deleted: boolean }> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    this.currentJob(id, expectedRevision)
    if (this.hasActiveRun(id)) {
      cronFailure('Cron job cannot be deleted while an occurrence is active', 'CRON_JOB_ACTIVE')
    }
    const result = { id, deleted: this.store.deleteJob(id) }
    this.requestDrive()
    return result
  }

  async runNow(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronRun> {
    await this.awaitStartup()
    this.assertOperational()
    this.assertAuthority(authority)
    const job = this.currentJob(id, expectedRevision)
    let scheduledAt = iso(this.now())
    // Two manual requests in one millisecond remain distinct without weakening
    // deterministic identity for either accepted occurrence.
    while (this.store.getRun(deterministicRunId(id, scheduledAt, 'manual')) !== undefined) {
      scheduledAt = iso(Date.parse(scheduledAt) + 1)
    }
    const sessionId = deterministicSessionId(id, scheduledAt, 'manual')
    const claimed = this.store.claimManual(job, scheduledAt, scheduledAt, sessionId)
    if (claimed.kind === 'stale' || claimed.run === undefined) {
      cronFailure(`Cron job ${id} changed while claiming a manual run`, 'CRON_REVISION_CONFLICT')
    }
    if (claimed.kind === 'claimed') this.enqueue({ job, run: claimed.run, mode: 'create' })
    this.requestDrive()
    return claimed.run
  }

  async listRuns(query: CronRunQuery): Promise<readonly CronRun[]> {
    await this.awaitStartup()
    return this.store.listRuns(query.jobId, query.limit)
  }
}
