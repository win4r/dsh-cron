import { statSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { afterEach, beforeEach, describe, expect, test } from 'vitest'
import { deterministicSessionId } from '../src/cron.js'
import { CronStore } from '../src/store.js'
import type { CreateCronJobInput, CronJob, CronRunReceipt } from '../src/types.js'

const NOW = '2026-08-22T18:00:00.000Z'
const FIRST = '2026-08-22T18:01:00.000Z'
const SECOND = '2026-08-22T18:02:00.000Z'

function jobInput(overrides: Partial<CreateCronJobInput> = {}): CreateCronJobInput {
  return {
    id: 'daily-report',
    name: 'Daily report',
    expression: '* * * * *',
    timeZone: 'UTC',
    prompt: 'Generate the daily report.',
    cwd: '/tmp/workspace',
    agentPreset: 'standard',
    permissionPreset: 'workspace-write',
    allowedTools: ['read', 'write'],
    misfirePolicy: 'latest',
    overlapPolicy: 'forbid',
    maxRuntimeMs: 60_000,
    enabled: true,
    nextRunAt: FIRST,
    createdBySessionId: 'admin-session',
    now: NOW,
    ...overrides,
  }
}

const receipt: CronRunReceipt = {
  status: 'succeeded',
  summary: 'Report generated.',
  artifacts: ['/tmp/workspace/report.md'],
  checks: [{ name: 'exists', passed: true }],
}

describe('CronStore', () => {
  let directory: string
  let databasePath: string
  let store: CronStore | undefined

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'dsh-cron-store-'))
    databasePath = join(directory, 'cron.sqlite')
  })

  afterEach(async () => {
    store?.close()
    store = undefined
    await rm(directory, { recursive: true, force: true })
  })

  test('creates a private WAL database and health-check compatible heartbeat', () => {
    store = new CronStore(databasePath)
    expect(statSync(databasePath).mode & 0o777).toBe(0o600)
    expect(store.readHeartbeat()).toBeUndefined()

    store.writeHeartbeat({
      instanceId: 'host-a',
      pid: 1234,
      startedAt: NOW,
      heartbeatAt: FIRST,
    })
    expect(store.readHeartbeat()).toEqual({
      instanceId: 'host-a',
      pid: 1234,
      startedAt: NOW,
      heartbeatAt: FIRST,
    })

    const observer = new DatabaseSync(databasePath, { readOnly: true })
    try {
      expect(observer.prepare('PRAGMA journal_mode').get()).toMatchObject({ journal_mode: 'wal' })
      const row = observer.prepare(
        "SELECT value_json FROM cron_meta WHERE key = 'heartbeat'",
      ).get() as { value_json: string }
      expect(JSON.parse(row.value_json)).toMatchObject({ instanceId: 'host-a', heartbeatAt: FIRST })
    } finally {
      observer.close()
    }
  })

  test('persists jobs and enforces optimistic revisions', () => {
    store = new CronStore(databasePath)
    const created = store.createJob(jobInput())
    expect(created.revision).toBe(1)
    expect(store.dueJobs(NOW, 10)).toHaveLength(0)
    expect(store.dueJobs(FIRST, 10).map(job => job.id)).toEqual(['daily-report'])

    const updated = store.updateJob(created.id, {
      name: 'Updated report',
      provider: 'local',
      expectedRevision: 1,
      now: FIRST,
    })
    expect(updated).toMatchObject({ name: 'Updated report', provider: 'local', revision: 2 })
    expect(store.updateJob(created.id, {
      name: 'stale update',
      expectedRevision: 1,
      now: SECOND,
    })).toBeUndefined()

    store.close()
    store = new CronStore(databasePath)
    expect(store.getJob(created.id)).toMatchObject({ name: 'Updated report', revision: 2 })
  })

  test('atomically jumps from durable nextRunAt to the latest occurrence', () => {
    store = new CronStore(databasePath)
    const job = store.createJob(jobInput())
    const latest = '2026-08-22T18:59:00.000Z'
    const next = '2026-08-22T19:00:00.000Z'
    const now = '2026-08-22T18:59:30.000Z'
    const sessionId = deterministicSessionId(job.id, latest)

    const claimed = store.claimScheduled(job, latest, next, now, sessionId)
    expect(claimed.kind).toBe('claimed')
    expect(claimed.run).toMatchObject({
      jobId: job.id,
      scheduledAt: latest,
      state: 'claimed',
      sessionId,
    })
    expect(store.getJob(job.id)).toMatchObject({ nextRunAt: next, revision: 2 })

    // The same stale snapshot cannot claim or advance the occurrence twice.
    expect(store.claimScheduled(job, latest, next, now, sessionId)).toEqual({ kind: 'stale' })
  })

  test('advances and records a skipped run when overlap is forbidden', () => {
    store = new CronStore(databasePath)
    const job = store.createJob(jobInput())
    const manualAt = '2026-08-22T18:00:30.000Z'
    const manual = store.claimManual(
      job,
      manualAt,
      manualAt,
      deterministicSessionId(job.id, manualAt, 'manual'),
    )
    expect(manual.kind).toBe('claimed')

    const overlap = store.claimScheduled(
      job,
      FIRST,
      SECOND,
      FIRST,
      deterministicSessionId(job.id, FIRST),
    )
    expect(overlap.kind).toBe('overlap')
    expect(overlap.run).toMatchObject({
      state: 'skipped',
      finishReason: 'overlap_forbid',
      scheduledAt: FIRST,
    })
    expect(store.getJob(job.id)).toMatchObject({ nextRunAt: SECOND, revision: 2 })
    expect(store.activeRuns()).toHaveLength(1)
  })

  test('stores message admission and atomically terminates with a receipt', () => {
    store = new CronStore(databasePath)
    const job = store.createJob(jobInput())
    const result = store.claimScheduled(
      job,
      FIRST,
      SECOND,
      FIRST,
      deterministicSessionId(job.id, FIRST),
    )
    if (result.run === undefined) throw new Error('expected a claimed run')

    expect(store.markEnqueued(result.run.id, FIRST, 'message-1')).toMatchObject({
      state: 'enqueued',
      messageId: 'message-1',
    })
    expect(store.markEnqueued(result.run.id, FIRST, 'message-1')).toMatchObject({
      state: 'enqueued',
    })
    expect(() => store?.markEnqueued(result.run!.id, FIRST, 'message-2'))
      .toThrow(/different message/)
    expect(store.markRunning(result.run.id, FIRST)).toMatchObject({ state: 'running' })

    const completed = store.writeReceipt(result.run.id, receipt, SECOND)
    expect(completed).toMatchObject({
      state: 'succeeded',
      finishedAt: SECOND,
      finishReason: 'cron_complete',
      receipt,
    })
    expect(store.activeRuns()).toHaveLength(0)
    expect(store.writeReceipt(result.run.id, receipt, SECOND)).toEqual(completed)
    expect(() => store?.writeReceipt(result.run!.id, { ...receipt, summary: 'conflict' }, SECOND))
      .toThrow(/conflicting terminal receipt/)
  })

  test('keeps run audit history when its job definition is deleted', () => {
    store = new CronStore(databasePath)
    const job: CronJob = store.createJob(jobInput())
    const result = store.claimManual(
      job,
      NOW,
      NOW,
      deterministicSessionId(job.id, NOW, 'manual'),
    )
    if (result.run === undefined) throw new Error('expected a manual run')
    store.finishRun(result.run.id, 'unknown', FIRST, {
      finishReason: 'host_shutdown',
      error: 'Host stopped before a receipt was observed.',
    })

    expect(store.deleteJob(job.id)).toBe(true)
    expect(store.getJob(job.id)).toBeUndefined()
    expect(store.listRuns(job.id)).toEqual([
      expect.objectContaining({ id: result.run.id, state: 'unknown' }),
    ])
  })
})
