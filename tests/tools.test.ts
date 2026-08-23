import { afterEach, describe, expect, it, vi } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry, { Inbox } from '@deepseek-ai/dsh-agent'
import type { Agent, AgentCancelCause, InboxTarget } from '@deepseek-ai/dsh-agent'
import { CallId, createUserMessage } from '@deepseek-ai/dsh-llm'
import type { MessageSource, UserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import { createScope } from '@deepseek-ai/dsh-scope'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime from '@deepseek-ai/dsh-tools'
import type { ToolExecutionResult } from '@deepseek-ai/dsh-tools'
import type { CronJob, CronRun } from '../src/types.ts'
import { registerCronAdminTools } from '../src/tools.ts'
import type {
  CronAdminCreateInput,
  CronAdminRuntime,
  CronAdminUpdateInput,
  CronRunQuery,
} from '../src/tools.ts'
import type { CronAdminAuthority } from '../src/authority.ts'

const signal = new AbortController().signal
const contexts: Context[] = []

const JOB: CronJob = {
  id: 'job-1',
  name: 'Daily report',
  expression: '0 9 * * 1-5',
  timeZone: 'America/Los_Angeles',
  prompt: 'Build the daily report.',
  cwd: '/workspace',
  agentPreset: 'cron-runner',
  permissionPreset: 'read-only',
  allowedTools: ['read_file'],
  misfirePolicy: 'latest',
  overlapPolicy: 'forbid',
  maxRuntimeMs: 60_000,
  enabled: true,
  nextRunAt: '2026-08-24T16:00:00.000Z',
  createdBySessionId: 'admin-root',
  createdAt: '2026-08-22T12:00:00.000Z',
  updatedAt: '2026-08-22T12:00:00.000Z',
  revision: 1,
}

const RUN: CronRun = {
  id: 'job-1:2026-08-22T12:00:00.000Z',
  jobId: 'job-1',
  scheduledAt: '2026-08-22T12:00:00.000Z',
  trigger: 'manual',
  state: 'claimed',
  sessionId: 'cron-job-1-manual',
  attempt: 1,
  claimedAt: '2026-08-22T12:00:00.000Z',
}

interface RuntimeSpies extends CronAdminRuntime {
  listJobs: ReturnType<typeof vi.fn<() => Promise<readonly CronJob[]>>>
  createJob: ReturnType<typeof vi.fn<(
    input: CronAdminCreateInput,
    authority: CronAdminAuthority,
  ) => Promise<CronJob>>>
  updateJob: ReturnType<typeof vi.fn<(
    id: string,
    input: CronAdminUpdateInput,
    authority: CronAdminAuthority,
  ) => Promise<CronJob>>>
  pauseJob: ReturnType<typeof vi.fn<(
    id: string,
    revision: number,
    authority: CronAdminAuthority,
  ) => Promise<CronJob>>>
  resumeJob: RuntimeSpies['pauseJob']
  deleteJob: ReturnType<typeof vi.fn<(
    id: string,
    revision: number,
    authority: CronAdminAuthority,
  ) => Promise<{ readonly id: string; readonly deleted: boolean }>>>
  runNow: ReturnType<typeof vi.fn<(
    id: string,
    revision: number,
    authority: CronAdminAuthority,
  ) => Promise<CronRun>>>
  listRuns: ReturnType<typeof vi.fn<(query: CronRunQuery) => Promise<readonly CronRun[]>>>
}

interface Harness {
  readonly ctx: Context
  readonly agent: Agent
  readonly runtime: RuntimeSpies
  readonly disposeTools: () => void
}

function stubAgent(ctx: Context, scopedParent: Context, rawId: string): Agent {
  const session = ctx.sessions.create(SessionId(rawId))
  const inbox = new Inbox(session, { inserted: () => {}, discarded: () => {}, claimed: () => {} })
  const agent = {} as Agent
  const scope = createScope(scopedParent, agent)
  const agentCtx = scope.ctx.extend({ agent })
  Object.assign(agent, {
    id: session.id,
    options: {},
    session,
    inbox,
    status: 'running',
    ctx: agentCtx,
    send(_message: UserMessage, _target: InboxTarget, _wakeup: boolean) {},
    runMaintenance: task => task(signal),
    cancel(_cause: AgentCancelCause) {},
    whenIdle: () => Promise.resolve(),
    followup(_message: UserMessage) {},
    steer(_message: UserMessage) {},
    inject(_message: UserMessage) {},
  } satisfies Agent)
  return agent
}

function fakeRuntime(): RuntimeSpies {
  return {
    listJobs: vi.fn(async () => [JOB]),
    createJob: vi.fn(async () => JOB),
    updateJob: vi.fn(async () => ({ ...JOB, revision: 2 })),
    pauseJob: vi.fn(async () => ({ ...JOB, enabled: false, revision: 2 })),
    resumeJob: vi.fn(async () => ({ ...JOB, enabled: true, revision: 2 })),
    deleteJob: vi.fn(async id => ({ id, deleted: true })),
    runNow: vi.fn(async () => RUN),
    listRuns: vi.fn(async () => [RUN]),
  }
}

async function harness(): Promise<Harness> {
  const ctx = new Context()
  contexts.push(ctx)
  await ctx.plugin(SessionStore)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(SystemPrompt)
  await ctx.plugin(ToolRuntime)
  let scopedParent: Context | undefined
  const captureToolsContext = Object.assign((pluginCtx: Context) => {
    scopedParent = pluginCtx
  }, { inject: ['tools'] })
  await ctx.plugin(captureToolsContext)
  if (scopedParent === undefined) throw new Error('expected injected tool context')
  const agent = stubAgent(ctx, scopedParent, `cron-tools-${Math.random()}`)
  ctx.agents.register(agent)
  const runtime = fakeRuntime()
  const disposeTools = registerCronAdminTools(ctx, agent.ctx, agent, runtime)
  return { ctx, agent, runtime, disposeTools }
}

function openTurn(agent: Agent, source: MessageSource, text = 'manage cron'): void {
  const latestBoundary = [...agent.session.events].reverse().find(event =>
    event.type === 'turn/start' || event.type === 'turn/end')
  if (latestBoundary?.type === 'turn/start') {
    agent.session.append('turn/end', {
      turn: latestBoundary.data.turn,
      reason: { kind: 'completed' },
    })
  }
  const turn = agent.session.events
    .filter(event => event.type === 'turn/start')
    .reduce((max, event) => Math.max(max, event.data.turn), 0) + 1
  agent.inbox.append('next-turn', createUserMessage({
    content: [{ type: 'text', text }],
    source,
  }))
  const accepted = agent.inbox.claim('next-turn', turn)
  agent.session.append('turn/start', { turn })
  for (const message of accepted) {
    agent.session.append('user/message', message, { surfaceOp: 'append' })
  }
}

async function execute(
  test: Harness,
  name: string,
  args: unknown,
  initiator: Agent | null = test.agent,
): Promise<ToolExecutionResult> {
  const invoke = () => test.ctx.tools.execute({
    signal,
    callId: CallId(`call-${Math.random()}`),
    name,
    arguments: args,
    agent: test.agent,
  })
  return initiator === null ? invoke() : test.ctx.agents.withInitiator(initiator, invoke)
}

function value(result: ToolExecutionResult): unknown {
  expect(result.isError).toBe(false)
  if (result.isError) throw new Error('expected Cron tool success')
  const block = result.content[0]
  if (block?.type !== 'text') throw new Error('expected canonical JSON text')
  expect(JSON.parse(block.text)).toEqual(result.value)
  return result.value
}

function confirmationToken(result: ToolExecutionResult): string {
  expect(result.isError).toBe(true)
  expect(result.error?.info?.code).toBe('CRON_TOOL_CONFIRMATION_REQUIRED')
  const match = result.error?.message.match(/DSH_CRON_CONFIRM_v1_[0-9a-f]{64}/u)
  if (match === null || match === undefined) throw new Error('expected Cron confirmation token')
  return match[0]
}

async function confirmAndExecute(
  test: Harness,
  name: string,
  args: Readonly<Record<string, unknown>>,
): Promise<ToolExecutionResult> {
  const token = confirmationToken(await execute(test, name, args))
  openTurn(test.agent, { kind: 'user' }, `I directly confirm ${token}`)
  return execute(test, name, { ...args, confirmation_token: token })
}

afterEach(async () => {
  await Promise.allSettled(contexts.splice(0).map(ctx => ctx.fiber.dispose()))
})

describe('Cron administration tool scope and authority', () => {
  it('registers eight scoped tools and disposes them as one unit', async () => {
    const test = await harness()
    const names = [
      'cron_create', 'cron_list', 'cron_update', 'cron_pause',
      'cron_resume', 'cron_delete', 'cron_run_now', 'cron_runs',
    ]
    expect(names.map(name => test.ctx.tools.get(name, test.agent)?.name)).toEqual(names)
    expect(names.map(name => test.ctx.tools.get(name))).toEqual(names.map(() => undefined))
    test.disposeTools()
    test.disposeTools()
    expect(names.map(name => test.ctx.tools.get(name, test.agent))).toEqual(names.map(() => undefined))
  })

  it('requires a later argument-bound human confirmation before a normalized create', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' }, 'Create the requested daily report schedule.')
    const rawArgs = {
      name: ' Daily report ',
      expression: ' 0   9 * * 1-5 ',
      prompt: 'Build the daily report.',
      time_zone: 'US/Pacific',
      cwd: '/workspace',
      agent_preset: ' cron-runner ',
      permission_preset: ' read-only ',
      max_runtime_seconds: 60,
    }
    const first = await execute(test, 'cron_create', rawArgs)
    const token = confirmationToken(first)
    expect(test.runtime.createJob).not.toHaveBeenCalled()

    const canonicalArgs = {
      ...rawArgs,
      name: 'Daily report',
      expression: '0 9 * * 1-5',
      time_zone: 'America/Los_Angeles',
    }
    const sameTurn = await execute(test, 'cron_create', {
      ...canonicalArgs,
      confirmation_token: token,
    })
    expect(confirmationToken(sameTurn)).toBe(token)
    expect(test.runtime.createJob).not.toHaveBeenCalled()

    openTurn(test.agent, { kind: 'user' }, `Confirm this exact mutation: ${token}`)
    const result = await execute(test, 'cron_create', {
      ...canonicalArgs,
      confirmation_token: token,
    })

    expect(value(result)).toMatchObject({
      id: 'job-1',
      expression: '0 9 * * 1-5',
      timeZone: 'America/Los_Angeles',
      revision: 1,
    })
    expect(test.runtime.createJob).toHaveBeenCalledOnce()
    const [input, authority] = test.runtime.createJob.mock.calls[0] as [CronAdminCreateInput, CronAdminAuthority]
    expect(input).toEqual({
      name: 'Daily report',
      expression: '0 9 * * 1-5',
      prompt: 'Build the daily report.',
      timeZone: 'America/Los_Angeles',
      cwd: '/workspace',
      agentPreset: 'cron-runner',
      permissionPreset: 'read-only',
      maxRuntimeMs: 60_000,
    })
    expect(input).not.toHaveProperty('confirmation_token')
    expect(authority.agent).toBe(test.agent)
    expect(authority.sessionId).toBe(test.agent.id)
  })

  it.each([
    ['cron', {
      kind: 'cron',
      jobId: 'job-1',
      runId: 'run-1',
      scheduledAt: '2026-08-22T12:00:00.000Z',
    }],
    ['plugin', { kind: 'plugin', plugin: 'test' }],
    ['goal', { kind: 'goal', goalId: 'goal-1', revision: 1, round: 1 }],
  ] as const)('rejects %s-sourced writes before issuing confirmation', async (_kind, rawSource) => {
    const test = await harness()
    openTurn(test.agent, rawSource as MessageSource)
    const nonHuman = await execute(test, 'cron_pause', { id: 'job-1', expected_revision: 1 })
    expect(nonHuman.error?.info?.code).toBe('CRON_TOOL_AUTHORITY_REQUIRED')
    expect(test.runtime.pauseJob).not.toHaveBeenCalled()
  })

  it('rejects driverless calls before reaching the runtime', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' })
    const driverless = await execute(test, 'cron_list', {}, null)
    expect(driverless.error?.info?.code).toBe('CRON_TOOL_DRIVER_REQUIRED')
    expect(test.runtime.listJobs).not.toHaveBeenCalled()
  })

  it('permits read-only inspection in a non-human root turn', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'plugin', plugin: 'test' })
    expect(value(await execute(test, 'cron_list', {}))).toEqual([
      expect.objectContaining({ id: 'job-1', revision: 1 }),
    ])
    expect(value(await execute(test, 'cron_runs', { job_id: 'job-1', limit: 5 }))).toEqual([
      expect.objectContaining({ id: RUN.id, state: 'claimed' }),
    ])
    expect(test.runtime.listRuns).toHaveBeenCalledWith({ jobId: 'job-1', limit: 5 })
  })

  it('rejects unknown arguments, non-strict expressions, and non-integral revisions', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' })

    const unknown = await execute(test, 'cron_create', {
      name: 'job', expression: '0 9 * * *', prompt: 'work', allowed_tools: ['bash'],
    })
    expect(unknown.error?.info?.code).toBe('CRON_TOOL_INVALID_ARGUMENT')

    const seconds = await execute(test, 'cron_create', {
      name: 'job', expression: '0 0 9 * * *', prompt: 'work',
    })
    expect(seconds.error?.info?.code).toBe('CRON_TOOL_INVALID_ARGUMENT')

    const ambiguousDays = await execute(test, 'cron_create', {
      name: 'job', expression: '0 9 1 * 1', prompt: 'work',
    })
    expect(ambiguousDays.error?.info?.code).toBe('CRON_TOOL_INVALID_ARGUMENT')

    const staleShape = await execute(test, 'cron_delete', { id: 'job-1', expected_revision: 1.5 })
    expect(staleShape.error?.info?.code).toBe('CRON_TOOL_INVALID_ARGUMENT')
    expect(test.runtime.createJob).not.toHaveBeenCalled()
    expect(test.runtime.deleteJob).not.toHaveBeenCalled()
  })

  it('passes exact revisions and explicit selector clears to compare-and-set mutations', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' })
    await confirmAndExecute(test, 'cron_update', {
      id: 'job-1', expected_revision: 1, provider: null, model: null, misfire_policy: 'skip',
    })
    const updateCall = test.runtime.updateJob.mock.calls[0]
    expect(updateCall?.[0]).toBe('job-1')
    expect(updateCall?.[1]).toEqual({
      expectedRevision: 1, provider: null, model: null, misfirePolicy: 'skip',
    })
    expect(updateCall?.[2].agent).toBe(test.agent)

    await confirmAndExecute(test, 'cron_run_now', { id: 'job-1', expected_revision: 1 })
    const runCall = test.runtime.runNow.mock.calls[0]
    expect(runCall?.[0]).toBe('job-1')
    expect(runCall?.[1]).toBe(1)
    expect(runCall?.[2].agent).toBe(test.agent)
  })

  it('does not let a confirmed token authorize changed normalized arguments', async () => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' }, 'Pause job-1 at revision 1.')
    const original = { id: 'job-1', expected_revision: 1 }
    const token = confirmationToken(await execute(test, 'cron_pause', original))

    openTurn(test.agent, { kind: 'user' }, `I confirm ${token}`)
    const changed = await execute(test, 'cron_pause', {
      id: 'job-1',
      expected_revision: 2,
      confirmation_token: token,
    })
    expect(confirmationToken(changed)).not.toBe(token)
    expect(test.runtime.pauseJob).not.toHaveBeenCalled()
  })

  it('binds confirmation tokens to the exact operation and root session', async () => {
    const first = await harness()
    const second = await harness()
    const args = { id: 'job-1', expected_revision: 1 }
    openTurn(first.agent, { kind: 'user' })
    openTurn(second.agent, { kind: 'user' })
    const firstToken = confirmationToken(await execute(first, 'cron_pause', args))
    const secondToken = confirmationToken(await execute(second, 'cron_pause', args))
    expect(secondToken).not.toBe(firstToken)

    openTurn(first.agent, { kind: 'user' }, `I confirm ${firstToken}`)
    const otherOperation = await execute(first, 'cron_resume', {
      ...args,
      confirmation_token: firstToken,
    })
    expect(confirmationToken(otherOperation)).not.toBe(firstToken)
    expect(first.runtime.pauseJob).not.toHaveBeenCalled()
    expect(first.runtime.resumeJob).not.toHaveBeenCalled()
  })

  it.each([
    ['tool output', { kind: 'tool', callId: CallId('untrusted-tool-result') }],
    ['model content', { kind: 'model', provider: 'test', model: 'test' }],
  ] as const)('does not accept the token from later %s instead of a human message', async (_kind, source) => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' }, 'Pause job-1.')
    const args = { id: 'job-1', expected_revision: 1 }
    const token = confirmationToken(await execute(test, 'cron_pause', args))

    openTurn(test.agent, source, token)
    const result = await execute(test, 'cron_pause', {
      ...args,
      confirmation_token: token,
    })
    expect(result.error?.info?.code).toBe('CRON_TOOL_AUTHORITY_REQUIRED')
    expect(test.runtime.pauseJob).not.toHaveBeenCalled()
  })

  it.each([
    ['cron_create', { name: 'job', expression: '0 9 * * *', prompt: 'work' }, 'createJob'],
    ['cron_update', { id: 'job-1', expected_revision: 1, name: 'renamed' }, 'updateJob'],
    ['cron_pause', { id: 'job-1', expected_revision: 1 }, 'pauseJob'],
    ['cron_resume', { id: 'job-1', expected_revision: 1 }, 'resumeJob'],
    ['cron_delete', { id: 'job-1', expected_revision: 1 }, 'deleteJob'],
    ['cron_run_now', { id: 'job-1', expected_revision: 1 }, 'runNow'],
  ] as const)('gates mutating tool %s on its first direct-human attempt', async (name, args, spy) => {
    const test = await harness()
    openTurn(test.agent, { kind: 'user' })
    confirmationToken(await execute(test, name, args))
    expect(test.runtime[spy]).not.toHaveBeenCalled()
  })
})
