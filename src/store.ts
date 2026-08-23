import {
  chmodSync,
  closeSync,
  constants,
  existsSync,
  fchmodSync,
  mkdirSync,
  openSync,
} from 'node:fs'
import { dirname, resolve } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { deterministicRunId } from './cron.js'
import {
  TERMINAL_RUN_STATES,
  type ClaimResult,
  type CreateCronJobInput,
  type CronHeartbeat,
  type CronJob,
  type CronRun,
  type CronRunReceipt,
  type CronStoreApi,
  type RunState,
  type RunTrigger,
  type UpdateCronJobInput,
} from './types.js'

const SCHEMA_VERSION = 1
const DEFAULT_BUSY_TIMEOUT_MS = 5_000
const DEFAULT_RUN_LIMIT = 100
const MAX_RUN_LIMIT = 1_000
const ACTIVE_RUN_STATES: readonly RunState[] = ['claimed', 'enqueued', 'running']
const TERMINAL_RUN_STATE_SET = new Set<RunState>(TERMINAL_RUN_STATES)

type SqlRow = Record<string, unknown>

export interface CronStoreOptions {
  readonly path: string
  readonly busyTimeoutMs?: number
}

function canonicalInstant(value: string, label: string): string {
  const parsed = new Date(value)
  if (!Number.isFinite(parsed.getTime()) || parsed.toISOString() !== value) {
    throw new TypeError(`${label} must be a canonical UTC ISO instant`)
  }
  return value
}

function requiredString(row: SqlRow, key: string): string {
  const value = row[key]
  if (typeof value !== 'string') throw new Error(`corrupt cron database: ${key} is not text`)
  return value
}

function optionalString(row: SqlRow, key: string): string | undefined {
  const value = row[key]
  if (value === null || value === undefined) return undefined
  if (typeof value !== 'string') throw new Error(`corrupt cron database: ${key} is not text`)
  return value
}

function requiredNumber(row: SqlRow, key: string): number {
  const value = row[key]
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new Error(`corrupt cron database: ${key} is not numeric`)
  }
  return value
}

function parseAllowedTools(value: string): readonly string[] {
  const parsed: unknown = JSON.parse(value)
  if (!Array.isArray(parsed) || parsed.some(item => typeof item !== 'string')) {
    throw new Error('corrupt cron database: allowed_tools_json is not a string array')
  }
  return parsed
}

function normalizeReceipt(value: unknown): CronRunReceipt {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('receipt must be an object')
  }
  const record = value as Record<string, unknown>
  if (record.status !== 'succeeded' && record.status !== 'failed' && record.status !== 'needs_review') {
    throw new TypeError('receipt.status is invalid')
  }
  if (typeof record.summary !== 'string') throw new TypeError('receipt.summary must be a string')
  if (!Array.isArray(record.artifacts) || record.artifacts.some(item => typeof item !== 'string')) {
    throw new TypeError('receipt.artifacts must be a string array')
  }
  if (!Array.isArray(record.checks)) throw new TypeError('receipt.checks must be an array')

  const checks = record.checks.map((value, index) => {
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      throw new TypeError(`receipt.checks[${index}] must be an object`)
    }
    const check = value as Record<string, unknown>
    if (typeof check.name !== 'string' || typeof check.passed !== 'boolean') {
      throw new TypeError(`receipt.checks[${index}] is invalid`)
    }
    if (check.detail !== undefined && typeof check.detail !== 'string') {
      throw new TypeError(`receipt.checks[${index}].detail must be a string`)
    }
    return {
      name: check.name,
      passed: check.passed,
      ...(check.detail === undefined ? {} : { detail: check.detail }),
    }
  })

  return {
    status: record.status,
    summary: record.summary,
    artifacts: [...record.artifacts] as string[],
    checks,
  }
}

function serializedReceipt(receipt: CronRunReceipt): string {
  return JSON.stringify(normalizeReceipt(receipt))
}

function asJob(row: SqlRow): CronJob {
  const provider = optionalString(row, 'provider')
  const model = optionalString(row, 'model')
  const nextRunAt = optionalString(row, 'next_run_at')
  return {
    id: requiredString(row, 'id'),
    name: requiredString(row, 'name'),
    expression: requiredString(row, 'expression'),
    timeZone: requiredString(row, 'time_zone'),
    prompt: requiredString(row, 'prompt'),
    cwd: requiredString(row, 'cwd'),
    agentPreset: requiredString(row, 'agent_preset'),
    permissionPreset: requiredString(row, 'permission_preset'),
    ...(provider === undefined ? {} : { provider }),
    ...(model === undefined ? {} : { model }),
    allowedTools: parseAllowedTools(requiredString(row, 'allowed_tools_json')),
    misfirePolicy: requiredString(row, 'misfire_policy') as CronJob['misfirePolicy'],
    overlapPolicy: requiredString(row, 'overlap_policy') as CronJob['overlapPolicy'],
    maxRuntimeMs: requiredNumber(row, 'max_runtime_ms'),
    enabled: requiredNumber(row, 'enabled') === 1,
    ...(nextRunAt === undefined ? {} : { nextRunAt }),
    createdBySessionId: requiredString(row, 'created_by_session_id'),
    createdAt: requiredString(row, 'created_at'),
    updatedAt: requiredString(row, 'updated_at'),
    revision: requiredNumber(row, 'revision'),
  }
}

function asRun(row: SqlRow): CronRun {
  const messageId = optionalString(row, 'message_id')
  const enqueuedAt = optionalString(row, 'enqueued_at')
  const startedAt = optionalString(row, 'started_at')
  const finishedAt = optionalString(row, 'finished_at')
  const finishReason = optionalString(row, 'finish_reason')
  const receiptJson = optionalString(row, 'receipt_json')
  const error = optionalString(row, 'error')
  return {
    id: requiredString(row, 'id'),
    jobId: requiredString(row, 'job_id'),
    scheduledAt: requiredString(row, 'scheduled_at'),
    trigger: requiredString(row, 'trigger') as RunTrigger,
    state: requiredString(row, 'state') as RunState,
    sessionId: requiredString(row, 'session_id'),
    ...(messageId === undefined ? {} : { messageId }),
    attempt: requiredNumber(row, 'attempt'),
    claimedAt: requiredString(row, 'claimed_at'),
    ...(enqueuedAt === undefined ? {} : { enqueuedAt }),
    ...(startedAt === undefined ? {} : { startedAt }),
    ...(finishedAt === undefined ? {} : { finishedAt }),
    ...(finishReason === undefined ? {} : { finishReason }),
    ...(receiptJson === undefined ? {} : { receipt: normalizeReceipt(JSON.parse(receiptJson)) }),
    ...(error === undefined ? {} : { error }),
  }
}

function validateLimit(limit: number, label = 'limit'): number {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_RUN_LIMIT) {
    throw new RangeError(`${label} must be an integer between 1 and ${MAX_RUN_LIMIT}`)
  }
  return limit
}

function ensurePrivateDatabaseFile(path: string): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 })
  const noFollow = typeof constants.O_NOFOLLOW === 'number' ? constants.O_NOFOLLOW : 0
  const descriptor = openSync(path, constants.O_CREAT | constants.O_RDWR | noFollow, 0o600)
  try {
    fchmodSync(descriptor, 0o600)
  } finally {
    closeSync(descriptor)
  }
}

function chmodIfPresent(path: string): void {
  if (existsSync(path)) chmodSync(path, 0o600)
}

/** Synchronous transactional SQLite store owned by the always-on Host plugin. */
export class CronStore implements CronStoreApi {
  readonly path: string
  readonly #database: DatabaseSync

  constructor(pathOrOptions: string | CronStoreOptions) {
    const options = typeof pathOrOptions === 'string' ? { path: pathOrOptions } : pathOrOptions
    const busyTimeoutMs = options.busyTimeoutMs ?? DEFAULT_BUSY_TIMEOUT_MS
    if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0 || busyTimeoutMs > 60_000) {
      throw new RangeError('busyTimeoutMs must be an integer between 0 and 60000')
    }
    if (options.path.trim() === '') throw new TypeError('database path must not be empty')

    this.path = options.path === ':memory:' ? ':memory:' : resolve(options.path)
    if (this.path !== ':memory:') ensurePrivateDatabaseFile(this.path)

    this.#database = new DatabaseSync(this.path, {
      allowExtension: false,
      enableDoubleQuotedStringLiterals: false,
      enableForeignKeyConstraints: true,
    })
    this.#database.exec(`
      PRAGMA busy_timeout = ${busyTimeoutMs};
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      PRAGMA foreign_keys = ON;
      PRAGMA trusted_schema = OFF;
    `)
    this.#migrate()

    if (this.path !== ':memory:') {
      chmodSync(this.path, 0o600)
      chmodIfPresent(`${this.path}-wal`)
      chmodIfPresent(`${this.path}-shm`)
    }
  }

  #migrate(): void {
    const versionRow = this.#database.prepare('PRAGMA user_version').get() as SqlRow | undefined
    const version = versionRow === undefined ? 0 : requiredNumber(versionRow, 'user_version')
    if (version > SCHEMA_VERSION) {
      throw new Error(`cron database schema ${version} is newer than supported schema ${SCHEMA_VERSION}`)
    }

    if (version === 0) {
      this.#transaction(() => {
        this.#database.exec(`
          CREATE TABLE cron_jobs (
            id TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
            name TEXT NOT NULL,
            expression TEXT NOT NULL,
            time_zone TEXT NOT NULL,
            prompt TEXT NOT NULL,
            cwd TEXT NOT NULL,
            agent_preset TEXT NOT NULL,
            permission_preset TEXT NOT NULL,
            provider TEXT,
            model TEXT,
            allowed_tools_json TEXT NOT NULL CHECK (json_valid(allowed_tools_json)),
            misfire_policy TEXT NOT NULL CHECK (misfire_policy IN ('latest', 'skip')),
            overlap_policy TEXT NOT NULL CHECK (overlap_policy = 'forbid'),
            max_runtime_ms INTEGER NOT NULL CHECK (max_runtime_ms > 0),
            enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
            next_run_at TEXT,
            created_by_session_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision >= 1)
          ) STRICT;

          CREATE TABLE cron_runs (
            id TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
            job_id TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            trigger TEXT NOT NULL CHECK (trigger IN ('schedule', 'manual')),
            state TEXT NOT NULL CHECK (state IN (
              'claimed', 'enqueued', 'running', 'succeeded', 'failed',
              'needs_review', 'incomplete', 'timed_out', 'unknown', 'skipped'
            )),
            session_id TEXT NOT NULL,
            message_id TEXT,
            attempt INTEGER NOT NULL CHECK (attempt >= 1),
            claimed_at TEXT NOT NULL,
            enqueued_at TEXT,
            started_at TEXT,
            finished_at TEXT,
            finish_reason TEXT,
            receipt_json TEXT CHECK (receipt_json IS NULL OR json_valid(receipt_json)),
            error TEXT,
            UNIQUE (job_id, scheduled_at, trigger)
          ) STRICT;

          CREATE TABLE cron_meta (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL CHECK (json_valid(value_json))
          ) STRICT;

          CREATE INDEX cron_jobs_due
            ON cron_jobs (next_run_at, id)
            WHERE enabled = 1 AND next_run_at IS NOT NULL;
          CREATE INDEX cron_runs_job_history
            ON cron_runs (job_id, scheduled_at DESC, claimed_at DESC);
          CREATE UNIQUE INDEX cron_runs_one_active_per_job
            ON cron_runs (job_id)
            WHERE state IN ('claimed', 'enqueued', 'running');
          CREATE UNIQUE INDEX cron_runs_message_id
            ON cron_runs (message_id)
            WHERE message_id IS NOT NULL;
          PRAGMA user_version = 1;
        `)
      })
    }
  }

  #transaction<T>(operation: () => T): T {
    this.#database.exec('BEGIN IMMEDIATE')
    try {
      const result = operation()
      this.#database.exec('COMMIT')
      return result
    } catch (error) {
      if (this.#database.isTransaction) this.#database.exec('ROLLBACK')
      throw error
    }
  }

  #jobRow(id: string): SqlRow | undefined {
    return this.#database.prepare('SELECT * FROM cron_jobs WHERE id = ?').get(id) as SqlRow | undefined
  }

  #runRow(id: string): SqlRow | undefined {
    return this.#database.prepare('SELECT * FROM cron_runs WHERE id = ?').get(id) as SqlRow | undefined
  }

  #hasActiveRun(jobId: string): boolean {
    return this.#database.prepare(`
      SELECT 1 AS found FROM cron_runs
      WHERE job_id = ? AND state IN ('claimed', 'enqueued', 'running')
      LIMIT 1
    `).get(jobId) !== undefined
  }

  #insertRun(input: {
    readonly jobId: string
    readonly scheduledAt: string
    readonly trigger: RunTrigger
    readonly state: 'claimed' | 'skipped'
    readonly sessionId: string
    readonly now: string
    readonly finishReason?: string
  }): CronRun {
    const id = deterministicRunId(input.jobId, input.scheduledAt, input.trigger)
    this.#database.prepare(`
      INSERT INTO cron_runs (
        id, job_id, scheduled_at, trigger, state, session_id, attempt,
        claimed_at, finished_at, finish_reason
      ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
    `).run(
      id,
      input.jobId,
      input.scheduledAt,
      input.trigger,
      input.state,
      input.sessionId,
      input.now,
      input.state === 'skipped' ? input.now : null,
      input.finishReason ?? null,
    )
    const row = this.#runRow(id)
    if (row === undefined) throw new Error('failed to read claimed cron run')
    return asRun(row)
  }

  listJobs(): readonly CronJob[] {
    return (this.#database.prepare('SELECT * FROM cron_jobs ORDER BY created_at, id').all() as SqlRow[])
      .map(asJob)
  }

  getJob(id: string): CronJob | undefined {
    const row = this.#jobRow(id)
    return row === undefined ? undefined : asJob(row)
  }

  createJob(input: CreateCronJobInput): CronJob {
    canonicalInstant(input.now, 'now')
    if (input.nextRunAt !== undefined) canonicalInstant(input.nextRunAt, 'nextRunAt')
    this.#database.prepare(`
      INSERT INTO cron_jobs (
        id, name, expression, time_zone, prompt, cwd, agent_preset,
        permission_preset, provider, model, allowed_tools_json,
        misfire_policy, overlap_policy, max_runtime_ms, enabled, next_run_at,
        created_by_session_id, created_at, updated_at, revision
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
    `).run(
      input.id,
      input.name,
      input.expression,
      input.timeZone,
      input.prompt,
      input.cwd,
      input.agentPreset,
      input.permissionPreset,
      input.provider ?? null,
      input.model ?? null,
      JSON.stringify(input.allowedTools),
      input.misfirePolicy,
      input.overlapPolicy,
      input.maxRuntimeMs,
      input.enabled ? 1 : 0,
      input.nextRunAt ?? null,
      input.createdBySessionId,
      input.now,
      input.now,
    )
    const created = this.getJob(input.id)
    if (created === undefined) throw new Error('failed to read created cron job')
    return created
  }

  updateJob(id: string, input: UpdateCronJobInput): CronJob | undefined {
    canonicalInstant(input.now, 'now')
    if (input.nextRunAt !== undefined && input.nextRunAt !== null) {
      canonicalInstant(input.nextRunAt, 'nextRunAt')
    }

    return this.#transaction(() => {
      const row = this.#jobRow(id)
      if (row === undefined) return undefined
      const current = asJob(row)
      if (current.revision !== input.expectedRevision) return undefined

      const provider = input.provider === undefined ? current.provider : (input.provider ?? undefined)
      const model = input.model === undefined ? current.model : (input.model ?? undefined)
      const nextRunAt = input.nextRunAt === undefined
        ? current.nextRunAt
        : (input.nextRunAt ?? undefined)

      const result = this.#database.prepare(`
        UPDATE cron_jobs SET
          name = ?, expression = ?, time_zone = ?, prompt = ?, cwd = ?,
          agent_preset = ?, permission_preset = ?, provider = ?, model = ?,
          misfire_policy = ?, max_runtime_ms = ?, enabled = ?, next_run_at = ?,
          updated_at = ?, revision = revision + 1
        WHERE id = ? AND revision = ?
      `).run(
        input.name ?? current.name,
        input.expression ?? current.expression,
        input.timeZone ?? current.timeZone,
        input.prompt ?? current.prompt,
        input.cwd ?? current.cwd,
        input.agentPreset ?? current.agentPreset,
        input.permissionPreset ?? current.permissionPreset,
        provider ?? null,
        model ?? null,
        input.misfirePolicy ?? current.misfirePolicy,
        input.maxRuntimeMs ?? current.maxRuntimeMs,
        (input.enabled ?? current.enabled) ? 1 : 0,
        nextRunAt ?? null,
        input.now,
        id,
        input.expectedRevision,
      )
      if (result.changes !== 1) return undefined
      const updated = this.#jobRow(id)
      return updated === undefined ? undefined : asJob(updated)
    })
  }

  deleteJob(id: string): boolean {
    return this.#database.prepare('DELETE FROM cron_jobs WHERE id = ?').run(id).changes === 1
  }

  dueJobs(now: string, limit: number): readonly CronJob[] {
    canonicalInstant(now, 'now')
    validateLimit(limit)
    return (this.#database.prepare(`
      SELECT * FROM cron_jobs
      WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ?
      ORDER BY next_run_at, id
      LIMIT ?
    `).all(now, limit) as SqlRow[]).map(asJob)
  }

  claimScheduled(
    job: CronJob,
    scheduledAt: string,
    nextRunAt: string,
    now: string,
    sessionId: string,
  ): ClaimResult {
    canonicalInstant(scheduledAt, 'scheduledAt')
    canonicalInstant(nextRunAt, 'nextRunAt')
    canonicalInstant(now, 'now')
    if (nextRunAt <= scheduledAt) throw new RangeError('nextRunAt must be after scheduledAt')
    if (scheduledAt > now) throw new RangeError('scheduledAt must not be in the future')
    const expectedNextRunAt = job.nextRunAt
    if (expectedNextRunAt === undefined || scheduledAt < expectedNextRunAt) {
      throw new RangeError('scheduledAt must not be before the durable nextRunAt')
    }

    return this.#transaction(() => {
      const row = this.#jobRow(job.id)
      if (row === undefined) return { kind: 'stale' }
      const current = asJob(row)
      const runId = deterministicRunId(job.id, scheduledAt, 'schedule')
      if (
        !current.enabled
        || current.revision !== job.revision
        || current.nextRunAt !== expectedNextRunAt
        || this.#runRow(runId) !== undefined
      ) {
        return { kind: 'stale' }
      }

      const overlap = this.#hasActiveRun(job.id)
      const advanced = this.#database.prepare(`
        UPDATE cron_jobs
        SET next_run_at = ?, updated_at = ?, revision = revision + 1
        WHERE id = ? AND revision = ? AND enabled = 1 AND next_run_at = ?
      `).run(nextRunAt, now, job.id, job.revision, expectedNextRunAt)
      if (advanced.changes !== 1) return { kind: 'stale' }

      if (overlap) {
        const run = this.#insertRun({
          jobId: job.id,
          scheduledAt,
          trigger: 'schedule',
          state: 'skipped',
          sessionId,
          now,
          finishReason: 'overlap_forbid',
        })
        return { kind: 'overlap', run }
      }

      const run = this.#insertRun({
        jobId: job.id,
        scheduledAt,
        trigger: 'schedule',
        state: 'claimed',
        sessionId,
        now,
      })
      return { kind: 'claimed', run }
    })
  }

  claimManual(
    job: CronJob,
    scheduledAt: string,
    now: string,
    sessionId: string,
  ): ClaimResult {
    canonicalInstant(scheduledAt, 'scheduledAt')
    canonicalInstant(now, 'now')
    if (scheduledAt > now) throw new RangeError('scheduledAt must not be in the future')

    return this.#transaction(() => {
      const row = this.#jobRow(job.id)
      if (row === undefined) return { kind: 'stale' }
      const current = asJob(row)
      const runId = deterministicRunId(job.id, scheduledAt, 'manual')
      if (current.revision !== job.revision || this.#runRow(runId) !== undefined) {
        return { kind: 'stale' }
      }

      if (this.#hasActiveRun(job.id)) {
        const run = this.#insertRun({
          jobId: job.id,
          scheduledAt,
          trigger: 'manual',
          state: 'skipped',
          sessionId,
          now,
          finishReason: 'overlap_forbid',
        })
        return { kind: 'overlap', run }
      }

      const run = this.#insertRun({
        jobId: job.id,
        scheduledAt,
        trigger: 'manual',
        state: 'claimed',
        sessionId,
        now,
      })
      return { kind: 'claimed', run }
    })
  }

  getRun(id: string): CronRun | undefined {
    const row = this.#runRow(id)
    return row === undefined ? undefined : asRun(row)
  }

  listRuns(jobId?: string, limit = DEFAULT_RUN_LIMIT): readonly CronRun[] {
    validateLimit(limit)
    const rows = jobId === undefined
      ? this.#database.prepare(`
          SELECT * FROM cron_runs ORDER BY claimed_at DESC, id DESC LIMIT ?
        `).all(limit)
      : this.#database.prepare(`
          SELECT * FROM cron_runs
          WHERE job_id = ?
          ORDER BY claimed_at DESC, id DESC
          LIMIT ?
        `).all(jobId, limit)
    return (rows as SqlRow[]).map(asRun)
  }

  activeRuns(): readonly CronRun[] {
    return (this.#database.prepare(`
      SELECT * FROM cron_runs
      WHERE state IN ('claimed', 'enqueued', 'running')
      ORDER BY claimed_at, id
    `).all() as SqlRow[]).map(asRun)
  }

  markEnqueued(runId: string, at: string, messageId?: string): CronRun | undefined {
    canonicalInstant(at, 'at')
    if (messageId !== undefined && messageId === '') throw new TypeError('messageId must not be empty')
    return this.#transaction(() => {
      const row = this.#runRow(runId)
      if (row === undefined) return undefined
      const current = asRun(row)
      if (current.state === 'enqueued') {
        if (messageId !== undefined && current.messageId !== messageId) {
          throw new Error(`run ${runId} is already associated with a different message`)
        }
        return current
      }
      if (current.state !== 'claimed') return undefined
      this.#database.prepare(`
        UPDATE cron_runs SET state = 'enqueued', enqueued_at = ?, message_id = ?
        WHERE id = ? AND state = 'claimed'
      `).run(at, messageId ?? null, runId)
      const updated = this.#runRow(runId)
      return updated === undefined ? undefined : asRun(updated)
    })
  }

  markRunning(runId: string, at: string): CronRun | undefined {
    canonicalInstant(at, 'at')
    return this.#transaction(() => {
      const row = this.#runRow(runId)
      if (row === undefined) return undefined
      const current = asRun(row)
      if (current.state === 'running') return current
      if (current.state !== 'claimed' && current.state !== 'enqueued') return undefined
      this.#database.prepare(`
        UPDATE cron_runs SET state = 'running', started_at = ?
        WHERE id = ? AND state IN ('claimed', 'enqueued')
      `).run(at, runId)
      const updated = this.#runRow(runId)
      return updated === undefined ? undefined : asRun(updated)
    })
  }

  finishRun(
    runId: string,
    state: RunState,
    at: string,
    details: {
      readonly finishReason?: string
      readonly receipt?: CronRunReceipt
      readonly error?: string
    } = {},
  ): CronRun | undefined {
    canonicalInstant(at, 'at')
    if (!TERMINAL_RUN_STATE_SET.has(state)) throw new TypeError(`${state} is not a terminal run state`)
    const receiptJson = details.receipt === undefined ? undefined : serializedReceipt(details.receipt)
    if (details.receipt !== undefined && details.receipt.status !== state) {
      throw new TypeError('receipt.status must match the terminal run state')
    }

    return this.#transaction(() => {
      const row = this.#runRow(runId)
      if (row === undefined) return undefined
      const current = asRun(row)
      if (TERMINAL_RUN_STATE_SET.has(current.state)) {
        const sameReceipt = receiptJson === undefined
          || (current.receipt !== undefined && serializedReceipt(current.receipt) === receiptJson)
        if (current.state === state && sameReceipt) return current
        throw new Error(`run ${runId} already has conflicting terminal state ${current.state}`)
      }

      this.#database.prepare(`
        UPDATE cron_runs
        SET state = ?, finished_at = ?, finish_reason = ?, receipt_json = ?, error = ?
        WHERE id = ? AND state IN ('claimed', 'enqueued', 'running')
      `).run(
        state,
        at,
        details.finishReason ?? null,
        receiptJson ?? null,
        details.error ?? null,
        runId,
      )
      const updated = this.#runRow(runId)
      return updated === undefined ? undefined : asRun(updated)
    })
  }

  writeReceipt(runId: string, receipt: CronRunReceipt, at: string): CronRun | undefined {
    canonicalInstant(at, 'at')
    const normalized = normalizeReceipt(receipt)
    const receiptJson = JSON.stringify(normalized)
    return this.#transaction(() => {
      const row = this.#runRow(runId)
      if (row === undefined) return undefined
      const current = asRun(row)
      if (TERMINAL_RUN_STATE_SET.has(current.state)) {
        const existingJson = current.receipt === undefined ? undefined : serializedReceipt(current.receipt)
        if (current.state === normalized.status && existingJson === receiptJson) return current
        throw new Error(`run ${runId} already has a conflicting terminal receipt`)
      }

      this.#database.prepare(`
        UPDATE cron_runs
        SET state = ?, finished_at = ?, finish_reason = 'cron_complete',
            receipt_json = ?, error = NULL
        WHERE id = ? AND state IN ('claimed', 'enqueued', 'running')
      `).run(normalized.status, at, receiptJson, runId)
      const updated = this.#runRow(runId)
      return updated === undefined ? undefined : asRun(updated)
    })
  }

  writeHeartbeat(heartbeat: CronHeartbeat): void {
    if (heartbeat.instanceId === '') throw new TypeError('heartbeat.instanceId must not be empty')
    if (!Number.isSafeInteger(heartbeat.pid) || heartbeat.pid <= 0) {
      throw new TypeError('heartbeat.pid must be a positive integer')
    }
    canonicalInstant(heartbeat.startedAt, 'heartbeat.startedAt')
    canonicalInstant(heartbeat.heartbeatAt, 'heartbeat.heartbeatAt')
    if (heartbeat.heartbeatAt < heartbeat.startedAt) {
      throw new RangeError('heartbeatAt must not be before startedAt')
    }
    const normalized: CronHeartbeat = {
      instanceId: heartbeat.instanceId,
      pid: heartbeat.pid,
      startedAt: heartbeat.startedAt,
      heartbeatAt: heartbeat.heartbeatAt,
    }
    this.#database.prepare(`
      INSERT INTO cron_meta (key, value_json) VALUES ('heartbeat', ?)
      ON CONFLICT (key) DO UPDATE SET value_json = excluded.value_json
    `).run(JSON.stringify(normalized))
  }

  readHeartbeat(): CronHeartbeat | undefined {
    const row = this.#database.prepare(
      "SELECT value_json FROM cron_meta WHERE key = 'heartbeat'",
    ).get() as SqlRow | undefined
    if (row === undefined) return undefined
    const parsed: unknown = JSON.parse(requiredString(row, 'value_json'))
    if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
      throw new Error('corrupt cron database: heartbeat is not an object')
    }
    const value = parsed as Record<string, unknown>
    if (
      typeof value.instanceId !== 'string'
      || typeof value.pid !== 'number'
      || typeof value.startedAt !== 'string'
      || typeof value.heartbeatAt !== 'string'
    ) {
      throw new Error('corrupt cron database: heartbeat fields are invalid')
    }
    const heartbeat: CronHeartbeat = {
      instanceId: value.instanceId,
      pid: value.pid,
      startedAt: value.startedAt,
      heartbeatAt: value.heartbeatAt,
    }
    canonicalInstant(heartbeat.startedAt, 'heartbeat.startedAt')
    canonicalInstant(heartbeat.heartbeatAt, 'heartbeat.heartbeatAt')
    return heartbeat
  }

  close(): void {
    if (this.#database.isOpen) this.#database.close()
  }
}

export { ACTIVE_RUN_STATES }
