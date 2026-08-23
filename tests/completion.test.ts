import { describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry, { Inbox } from '@deepseek-ai/dsh-agent'
import type { Agent, AgentCancelCause, InboxTarget } from '@deepseek-ai/dsh-agent'
import { CallId } from '@deepseek-ai/dsh-llm'
import type { UserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import { createScope } from '@deepseek-ai/dsh-scope'
import SystemPrompt from '@deepseek-ai/dsh-system-prompt'
import ToolRuntime, { defineTool } from '@deepseek-ai/dsh-tools'
import type { ToolExecutionResult } from '@deepseek-ai/dsh-tools'
import {
  CRON_COMPLETE_TOOL,
  registerCronCompletionTool,
} from '../src/completion.ts'
import { CronStore } from '../src/store.ts'
import type { CronRunReceipt } from '../src/types.ts'

const signal = new AbortController().signal
const CLAIMED_AT = '2026-08-22T12:00:00.000Z'
const FINISHED_AT = '2026-08-22T12:01:00.000Z'

const RECEIPT: CronRunReceipt = {
  status: 'succeeded',
  summary: 'Report generated and validated.',
  artifacts: ['/workspace/report.md'],
  checks: [{ name: 'report exists', passed: true }],
}

interface CompletionHarness {
  readonly ctx: Context
  readonly owner: Agent
  readonly store: CronStore
  readonly runId: string
  dispose(): Promise<void>
}

function stubAgent(ctx: Context, rawId: string): Agent {
  const session = ctx.sessions.create(SessionId(rawId))
  const inbox = new Inbox(session, { inserted: () => {}, discarded: () => {}, claimed: () => {} })
  const agent = {} as Agent
  const scope = createScope(ctx, agent)
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

async function harness(): Promise<CompletionHarness> {
  const root = new Context()
  await root.plugin(SessionStore)
  await root.plugin(AgentRegistry)
  await root.plugin(SystemPrompt)
  await root.plugin(ToolRuntime)

  let ctx: Context | undefined
  const captureInjectedContext = Object.assign((pluginCtx: Context) => {
    ctx = pluginCtx
  }, { inject: ['sessions', 'agents', 'tools'] })
  await root.plugin(captureInjectedContext)
  if (ctx === undefined) throw new Error('expected injected completion-test context')

  const owner = stubAgent(ctx, `cron-completion-${Math.random()}`)
  ctx.agents.register(owner)

  const store = new CronStore(':memory:')
  const job = store.createJob({
    id: 'job-completion',
    name: 'Completion integration',
    expression: '0 9 * * 1-5',
    timeZone: 'America/Los_Angeles',
    prompt: 'Generate the report.',
    cwd: '/workspace',
    agentPreset: 'cron-runner',
    permissionPreset: 'read-only',
    allowedTools: [],
    misfirePolicy: 'latest',
    overlapPolicy: 'forbid',
    maxRuntimeMs: 60_000,
    enabled: true,
    createdBySessionId: 'admin-root',
    now: CLAIMED_AT,
  })
  const claimed = store.claimManual(job, CLAIMED_AT, CLAIMED_AT, String(owner.id))
  if (claimed.kind !== 'claimed' || claimed.run === undefined) {
    throw new Error('expected claimed completion-test run')
  }

  return {
    ctx,
    owner,
    store,
    runId: claimed.run.id,
    async dispose() {
      store.close()
      await root.fiber.dispose()
    },
  }
}

function execute(
  test: CompletionHarness,
  name: string,
  args: unknown,
): Promise<ToolExecutionResult> {
  return test.ctx.tools.execute({
    signal,
    callId: CallId(`call-${Math.random()}`),
    name,
    arguments: args,
    agent: test.owner,
  })
}

describe('cron_complete durable commit', () => {
  it('commits one direct receipt to SQLite and rejects a repeated call', async () => {
    const test = await harness()
    try {
      const attachment = registerCronCompletionTool(
        test.ctx,
        test.owner.ctx,
        test.owner,
        test.runId,
        test.store,
        () => FINISHED_AT,
      )

      const first = await execute(test, CRON_COMPLETE_TOOL, RECEIPT)
      expect(first).toMatchObject({
        isError: false,
        value: { recorded: true },
        concludesTurn: true,
      })
      expect(attachment.captured()).toEqual(RECEIPT)
      expect(test.store.getRun(test.runId)).toMatchObject({
        state: 'succeeded',
        finishedAt: FINISHED_AT,
        finishReason: 'cron_complete',
        receipt: RECEIPT,
      })

      const duplicate = await execute(test, CRON_COMPLETE_TOOL, {
        ...RECEIPT,
        summary: 'A conflicting second result.',
      })
      expect(duplicate.isError).toBe(true)
      expect(test.store.getRun(test.runId)).toMatchObject({
        state: 'succeeded',
        receipt: RECEIPT,
      })
    } finally {
      await test.dispose()
    }
  })

  it('stages a nested receipt until its parent succeeds', async () => {
    const test = await harness()
    try {
      const attachment = registerCronCompletionTool(
        test.ctx,
        test.owner.ctx,
        test.owner,
        test.runId,
        test.store,
        () => FINISHED_AT,
      )

      const registerParent = (name: string, fail: boolean): void => {
        test.owner.ctx.tools.register(defineTool({
          name,
          description: 'Completion-test composite transport.',
          parameters: {},
          output: {
            schema: {
              type: 'object',
              additionalProperties: false,
              properties: { dispatched: { type: 'boolean', required: true } },
            },
            render: () => [{ type: 'text', text: 'Nested dispatch settled.' }],
          },
          async execute(_args, exec): Promise<{ dispatched: true }> {
            const nested = await test.ctx.tools.execute({
              signal: exec.signal,
              callId: CallId(`${String(exec.callId)}:nested`),
              rootCallId: exec.rootCallId,
              parent: exec.token,
              name: CRON_COMPLETE_TOOL,
              arguments: RECEIPT,
              agent: test.owner,
            })
            if (nested.isError) throw new Error(nested.error.message)
            if (nested.concludesTurn) exec.concludeTurn()
            if (fail) throw new Error('parent transport failed after nested success')
            return { dispatched: true }
          },
        }))
      }

      registerParent('completion_parent_fail', true)
      registerParent('completion_parent_success', false)

      const failedParent = await execute(test, 'completion_parent_fail', {})
      expect(failedParent.isError).toBe(true)
      expect(attachment.captured()).toBeUndefined()
      expect(test.store.getRun(test.runId)).toMatchObject({ state: 'claimed' })
      expect(test.store.getRun(test.runId)?.receipt).toBeUndefined()

      const successfulParent = await execute(test, 'completion_parent_success', {})
      expect(successfulParent).toMatchObject({ isError: false, concludesTurn: true })
      expect(attachment.captured()).toEqual(RECEIPT)
      expect(test.store.getRun(test.runId)).toMatchObject({
        state: 'succeeded',
        finishedAt: FINISHED_AT,
        finishReason: 'cron_complete',
        receipt: RECEIPT,
      })
    } finally {
      await test.dispose()
    }
  })
})
