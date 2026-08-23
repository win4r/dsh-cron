import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { CronAdminAuthority } from '../src/authority.ts'
import { deterministicSessionId, nextOccurrence } from '../src/cron.ts'
import type { ResolvedConfig } from '../src/config.ts'
import { CronRuntime } from '../src/runtime.ts'
import { CronStore } from '../src/store.ts'
import type { CreateCronJobInput, CronJob } from '../src/types.ts'

const NOW = Date.parse('2026-08-22T12:00:30.000Z')

function context(): Context {
  const agents = {
    withoutInitiator<T>(operation: () => T): T { return operation() },
    roots: () => [],
    get: () => undefined,
  }
  return {
    agents,
    sessionPersistence: {
      listSnapshots: async () => [],
    },
    logger: { warn: vi.fn() },
    get: () => undefined,
  } as unknown as Context
}

function config(): ResolvedConfig {
  return {
    databasePath: ':memory:',
    defaultTimeZone: 'UTC',
    defaultAgentPreset: 'standard',
    defaultPermissionPreset: 'workspace-write',
    defaultMisfirePolicy: 'latest',
    allowedTools: ['read'],
    maxConcurrentRuns: 1,
    maxRuntimeMs: 60_000,
    heartbeatIntervalMs: 30_000,
  }
}

function jobInput(overrides: Partial<CreateCronJobInput>): CreateCronJobInput {
  return {
    id: 'job',
    name: 'job',
    expression: '* * * * *',
    timeZone: 'UTC',
    prompt: 'read status',
    cwd: '/tmp',
    agentPreset: 'standard',
    permissionPreset: 'workspace-write',
    allowedTools: ['read'],
    misfirePolicy: 'skip',
    overlapPolicy: 'forbid',
    maxRuntimeMs: 60_000,
    enabled: true,
    nextRunAt: '2026-08-22T12:00:00.000Z',
    createdBySessionId: 'human',
    now: '2026-08-22T11:00:00.000Z',
    ...overrides,
  }
}

afterEach(() => {
  vi.useRealTimers()
})

describe('CronRuntime durable scheduling', () => {
  it('does not move nextRunAt when resume is repeated on an enabled job', async () => {
    const store = new CronStore(':memory:')
    const job = store.createJob(jobInput({
      id: 'already-enabled',
      nextRunAt: '2026-08-22T12:00:00.000Z',
    }))
    const agent = {
      id: 'admin-root',
      session: { id: 'admin-root' },
    } as unknown as Agent
    const ctx = {
      agents: {
        get: () => agent,
        roots: () => [agent],
        currentInitiator: () => agent,
      },
      logger: { warn: vi.fn() },
    } as unknown as Context
    const runtime = new CronRuntime(ctx, store, config(), {
      now: () => NOW,
      instanceId: 'test-instance',
    })
    ;(runtime as unknown as { startup: Promise<void> }).startup = Promise.resolve()
    const authority: CronAdminAuthority = {
      agent,
      sessionId: 'admin-root',
      signal: new AbortController().signal,
    }

    const resumed = await runtime.resumeJob(job.id, job.revision, authority)

    expect(resumed).toEqual(job)
    expect(store.getJob(job.id)).toMatchObject({
      enabled: true,
      nextRunAt: '2026-08-22T12:00:00.000Z',
      revision: 1,
    })

    await runtime.dispose()
    store.close()
  })

  it('records an overlap while global execution capacity is full', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    const store = new CronStore(':memory:')
    const job = store.createJob(jobInput({
      id: 'long-running',
      misfirePolicy: 'latest',
    }))
    const manualAt = '2026-08-22T11:59:30.000Z'
    const activeRun = store.claimManual(
      job,
      manualAt,
      manualAt,
      deterministicSessionId(job.id, manualAt, 'manual'),
    ).run
    if (activeRun === undefined) throw new Error('expected active manual run')

    const runtime = new CronRuntime(context(), store, config(), {
      now: () => NOW,
      instanceId: 'test-instance',
    })
    const internals = runtime as unknown as {
      active: Map<string, { promise: Promise<void> }>
      driveOnce(): void
    }
    internals.active.set(activeRun.id, { promise: new Promise(() => {}) })

    internals.driveOnce()

    expect(store.listRuns(job.id)).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: activeRun.id, state: 'claimed' }),
      expect.objectContaining({
        scheduledAt: '2026-08-22T12:00:00.000Z',
        state: 'skipped',
        finishReason: 'overlap_forbid',
      }),
    ]))
    expect(store.getJob(job.id)).toMatchObject({
      nextRunAt: '2026-08-22T12:01:00.000Z',
    })

    internals.active.clear()
    await runtime.dispose()
    store.close()
  })

  it('keeps the startup misfire cutoff across bounded due batches', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    const store = new CronStore(':memory:')
    // One more row than the runtime's bounded projection batch ensures the
    // final row is classified by a later timer-driven pump.
    for (let index = 0; index < 257; index += 1) {
      store.createJob(jobInput({
        id: `job-${String(index).padStart(3, '0')}`,
        nextRunAt: '2026-08-22T12:00:00.000Z',
      }))
    }
    const runtime = new CronRuntime(context(), store, config(), {
      now: () => NOW,
      instanceId: 'test-instance',
    })

    await runtime.start()
    expect(store.listRuns(undefined, 1_000)).toHaveLength(256)
    await vi.advanceTimersByTimeAsync(100)

    const runs = store.listRuns(undefined, 1_000)
    expect(runs).toHaveLength(257)
    expect(runs.every(run => run.state === 'skipped')).toBe(true)
    expect(runs.every(run => run.finishReason === 'misfire_skip')).toBe(true)
    // The last row had exactly one elapsed occurrence. It still counts as a
    // startup misfire even though the bounded batch deferred its claim.
    expect(runs.find(run => run.jobId === 'job-256')?.scheduledAt)
      .toBe('2026-08-22T12:00:00.000Z')

    await runtime.dispose()
    store.close()
  })

  it('marks an admitted run unknown when restart finds no durable Session', async () => {
    const store = new CronStore(':memory:')
    const job = store.createJob(jobInput({
      id: 'lost-session',
      enabled: false,
    })) as CronJob
    const scheduledAt = '2026-08-22T12:00:00.000Z'
    const claimed = store.claimManual(
      job,
      scheduledAt,
      scheduledAt,
      deterministicSessionId(job.id, scheduledAt, 'manual'),
    )
    expect(claimed.kind).toBe('claimed')
    const run = claimed.run
    if (run === undefined) throw new Error('test claim did not return a run')
    store.markEnqueued(run.id, scheduledAt, 'cron-message-id')

    const runtime = new CronRuntime(context(), store, config(), {
      now: () => NOW,
      instanceId: 'test-instance',
    })
    await runtime.start()

    expect(store.getRun(run.id)).toMatchObject({
      state: 'unknown',
      finishReason: 'recovery_session_missing',
    })

    await runtime.dispose()
    store.close()
  })

  it('stops refreshing the heartbeat after the scheduler pump faults', async () => {
    vi.useFakeTimers()
    vi.setSystemTime(NOW)
    const store = new CronStore(':memory:')
    const heartbeat = vi.spyOn(store, 'writeHeartbeat')
    vi.spyOn(store, 'dueJobs').mockImplementation(() => {
      throw new Error('synthetic durable projection failure')
    })
    const runtime = new CronRuntime(context(), store, {
      ...config(),
      heartbeatIntervalMs: 1_000,
    }, {
      now: Date.now,
      instanceId: 'faulted-instance',
    })

    await runtime.start()
    await vi.advanceTimersByTimeAsync(0)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(10_000)
    expect(heartbeat).toHaveBeenCalledTimes(1)

    await runtime.dispose()
    store.close()
  })
})
