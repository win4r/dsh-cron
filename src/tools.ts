/** Root-Agent-scoped management tools for Host-level durable Cron jobs. */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { HarnessError } from '@deepseek-ai/dsh-llm'
import type { ContentBlock } from '@deepseek-ai/dsh-llm'
import { defineTool } from '@deepseek-ai/dsh-tools'
import type { GenericCallView } from '@deepseek-ai/dsh-tools'
import { isAbsolute } from 'node:path'
import {
  cronAdminExecution,
  requireDirectHumanCronAuthority,
} from './authority.ts'
import type { CronAdminAuthority } from './authority.ts'
import type {
  CronJob,
  CronRun,
  MisfirePolicy,
} from './types.ts'
import { validateCronExpression } from './cron.ts'

const MAX_NAME_LENGTH = 200
const MAX_EXPRESSION_LENGTH = 256
const MAX_PROMPT_LENGTH = 100_000
const MAX_PATH_LENGTH = 4_096
const MAX_SELECTOR_LENGTH = 256
const MAX_RUNTIME_SECONDS = 7 * 24 * 60 * 60
const MAX_RUN_LIST_LIMIT = 100
const DEFAULT_RUN_LIST_LIMIT = 20
const JOB_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/

/** Input accepted by the runtime after model arguments are normalized. */
export interface CronAdminCreateInput {
  readonly name: string
  readonly expression: string
  readonly timeZone?: string
  readonly prompt: string
  readonly cwd?: string
  readonly agentPreset?: string
  readonly permissionPreset?: string
  readonly provider?: string
  readonly model?: string
  readonly misfirePolicy?: MisfirePolicy
  readonly maxRuntimeMs?: number
}

/** Mutable fields accepted by the runtime's compare-and-set update. */
export interface CronAdminUpdateInput {
  readonly expectedRevision: number
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
}

/** Bounded query for durable run history. */
export interface CronRunQuery {
  readonly jobId?: string
  readonly limit: number
}

/** Runtime surface consumed by the model-facing administration layer. */
export interface CronAdminRuntime {
  listJobs(): Promise<readonly CronJob[]>
  createJob(input: CronAdminCreateInput, authority: CronAdminAuthority): Promise<CronJob>
  updateJob(id: string, input: CronAdminUpdateInput, authority: CronAdminAuthority): Promise<CronJob>
  pauseJob(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronJob>
  resumeJob(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronJob>
  deleteJob(
    id: string,
    expectedRevision: number,
    authority: CronAdminAuthority,
  ): Promise<{ readonly id: string; readonly deleted: boolean }>
  runNow(id: string, expectedRevision: number, authority: CronAdminAuthority): Promise<CronRun>
  listRuns(query: CronRunQuery): Promise<readonly CronRun[]>
}

/** Model-visible job view; deployment-owned tool allowlists stay private. */
interface CronJobValue {
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
  readonly misfirePolicy: CronJob['misfirePolicy']
  readonly overlapPolicy: CronJob['overlapPolicy']
  readonly maxRuntimeMs: number
  readonly enabled: boolean
  readonly nextRunAt?: string
  readonly createdAt: string
  readonly updatedAt: string
  readonly revision: number
}

/** Model-visible run view with exact execution and receipt identity. */
interface CronRunValue {
  readonly id: string
  readonly jobId: string
  readonly scheduledAt: string
  readonly trigger: CronRun['trigger']
  readonly state: CronRun['state']
  readonly sessionId: string
  readonly messageId?: string
  readonly attempt: number
  readonly claimedAt: string
  readonly enqueuedAt?: string
  readonly startedAt?: string
  readonly finishedAt?: string
  readonly finishReason?: string
  readonly receipt?: {
    readonly status: 'succeeded' | 'failed' | 'needs_review'
    readonly summary: string
    readonly artifacts: string[]
    readonly checks: Array<{
      readonly name: string
      readonly passed: boolean
      readonly detail?: string
    }>
  }
  readonly error?: string
}

const CHECK_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    name: { type: 'string', required: true },
    passed: { type: 'boolean', required: true },
    detail: { type: 'string' },
  },
} as const

const RECEIPT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    status: { type: 'string', required: true, enum: ['succeeded', 'failed', 'needs_review'] },
    summary: { type: 'string', required: true },
    artifacts: { type: 'array', required: true, items: { type: 'string' } },
    checks: { type: 'array', required: true, items: CHECK_SCHEMA },
  },
} as const

const JOB_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    id: { type: 'string', required: true },
    name: { type: 'string', required: true },
    expression: { type: 'string', required: true },
    timeZone: { type: 'string', required: true },
    prompt: { type: 'string', required: true },
    cwd: { type: 'string', required: true },
    agentPreset: { type: 'string', required: true },
    permissionPreset: { type: 'string', required: true },
    provider: { type: 'string' },
    model: { type: 'string' },
    misfirePolicy: { type: 'string', required: true, enum: ['latest', 'skip'] },
    overlapPolicy: { type: 'string', required: true, const: 'forbid' },
    maxRuntimeMs: { type: 'integer', required: true },
    enabled: { type: 'boolean', required: true },
    nextRunAt: { type: 'string' },
    createdAt: { type: 'string', required: true },
    updatedAt: { type: 'string', required: true },
    revision: { type: 'integer', required: true },
  },
} as const

const RUN_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    id: { type: 'string', required: true },
    jobId: { type: 'string', required: true },
    scheduledAt: { type: 'string', required: true },
    trigger: { type: 'string', required: true, enum: ['schedule', 'manual'] },
    state: {
      type: 'string',
      required: true,
      enum: [
        'claimed',
        'enqueued',
        'running',
        'succeeded',
        'failed',
        'needs_review',
        'incomplete',
        'timed_out',
        'unknown',
        'skipped',
      ],
    },
    sessionId: { type: 'string', required: true },
    messageId: { type: 'string' },
    attempt: { type: 'integer', required: true },
    claimedAt: { type: 'string', required: true },
    enqueuedAt: { type: 'string' },
    startedAt: { type: 'string' },
    finishedAt: { type: 'string' },
    finishReason: { type: 'string' },
    receipt: RECEIPT_SCHEMA,
    error: { type: 'string' },
  },
} as const

const DELETE_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    id: { type: 'string', required: true },
    deleted: { type: 'boolean', required: true },
  },
} as const

const JOB_OUTPUT = { schema: JOB_SCHEMA, render: renderCanonicalJson }
const JOB_LIST_OUTPUT = {
  schema: { type: 'array', items: JOB_SCHEMA } as const,
  render: renderCanonicalJson,
}
const RUN_OUTPUT = { schema: RUN_SCHEMA, render: renderCanonicalJson }
const RUN_LIST_OUTPUT = {
  schema: { type: 'array', items: RUN_SCHEMA } as const,
  render: renderCanonicalJson,
}

/** Render one already-validated canonical value as compact deterministic JSON. */
function renderCanonicalJson(_args: unknown, value: unknown): ContentBlock[] {
  return [{ type: 'text', text: JSON.stringify(value) }]
}

/** Generic pending presentation without adding another data projection. */
function present(title: string, kind: 'read' | 'other', rawInput?: unknown): GenericCallView {
  return { card: 'generic', title, kind, ...rawInput === undefined ? {} : { rawInput } }
}

/** Throw one stable invalid-argument error after the schema-level type check. */
function invalid(message: string): never {
  throw new HarnessError(message, 'CRON_TOOL_INVALID_ARGUMENT')
}

/** The parameter root is intentionally open in defineTool; close it manually. */
function requireExactKeys(value: object, allowed: readonly string[]): void {
  const accepted = new Set(allowed)
  const unknown = Object.keys(value).filter(key => !accepted.has(key))
  if (unknown.length > 0) invalid(`unsupported cron argument(s): ${unknown.sort().join(', ')}`)
}

/** Normalize one bounded non-empty selector. */
function selector(value: string, field: string, max = MAX_SELECTOR_LENGTH): string {
  const normalized = value.trim()
  if (normalized.length === 0 || normalized.length > max || normalized.includes('\0')) {
    return invalid(`${field} must be a non-empty string no longer than ${max} characters`)
  }
  return normalized
}

/** Preserve prompt text while imposing a durable size and non-empty bound. */
function prompt(value: string): string {
  if (value.trim().length === 0 || value.length > MAX_PROMPT_LENGTH || value.includes('\0')) {
    return invalid(`prompt must be non-empty and no longer than ${MAX_PROMPT_LENGTH} characters`)
  }
  return value
}

/** Normalize and validate a five-field calendar expression. */
function expression(value: string, _timeZone?: string): string {
  const normalized = value.trim().split(/\s+/u).join(' ')
  if (normalized.length === 0 || normalized.length > MAX_EXPRESSION_LENGTH
    || normalized.split(' ').length !== 5) {
    return invalid('expression must be a valid five-field cron expression')
  }
  try {
    validateCronExpression(normalized)
  } catch {
    return invalid('expression must be a valid five-field cron expression')
  }
  return normalized
}

/** Canonicalize UTC or one runtime-supported IANA Area/Location name. */
function timeZone(value: string): string {
  const normalized = selector(value, 'time_zone')
  let canonical: string
  try {
    canonical = new Intl.DateTimeFormat('en-US', { timeZone: normalized }).resolvedOptions().timeZone
  } catch {
    return invalid('time_zone must be UTC or a valid IANA time-zone name')
  }
  return canonical
}

/** Validate one absolute working directory without performing filesystem I/O. */
function cwd(value: string): string {
  const normalized = selector(value, 'cwd', MAX_PATH_LENGTH)
  if (!isAbsolute(normalized)) invalid('cwd must be an absolute path')
  return normalized
}

/** Validate a positive compare-and-set revision. */
function revision(value: number): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    return invalid('expected_revision must be a positive safe integer')
  }
  return value
}

/** Validate a stable job identifier returned by Cron read tools. */
function jobId(value: string): string {
  if (!JOB_ID.test(value)) {
    return invalid('id must be the exact non-empty job id returned by a cron read tool')
  }
  return value
}

/** Convert a bounded model-facing seconds value to the runtime's milliseconds. */
function maxRuntimeMs(value: number): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_RUNTIME_SECONDS) {
    return invalid(`max_runtime_seconds must be a positive safe integer no greater than ${MAX_RUNTIME_SECONDS}`)
  }
  return value * 1_000
}

/** Project a host record without deployment-owned allowlists or creator identity. */
function jobValue(job: CronJob): CronJobValue {
  return {
    id: job.id,
    name: job.name,
    expression: job.expression,
    timeZone: job.timeZone,
    prompt: job.prompt,
    cwd: job.cwd,
    agentPreset: job.agentPreset,
    permissionPreset: job.permissionPreset,
    ...job.provider === undefined ? {} : { provider: job.provider },
    ...job.model === undefined ? {} : { model: job.model },
    misfirePolicy: job.misfirePolicy,
    overlapPolicy: job.overlapPolicy,
    maxRuntimeMs: job.maxRuntimeMs,
    enabled: job.enabled,
    ...job.nextRunAt === undefined ? {} : { nextRunAt: job.nextRunAt },
    createdAt: job.createdAt,
    updatedAt: job.updatedAt,
    revision: job.revision,
  }
}

/** Detach nested receipt arrays before returning a canonical run value. */
function runValue(run: CronRun): CronRunValue {
  return {
    id: run.id,
    jobId: run.jobId,
    scheduledAt: run.scheduledAt,
    trigger: run.trigger,
    state: run.state,
    sessionId: run.sessionId,
    ...run.messageId === undefined ? {} : { messageId: run.messageId },
    attempt: run.attempt,
    claimedAt: run.claimedAt,
    ...run.enqueuedAt === undefined ? {} : { enqueuedAt: run.enqueuedAt },
    ...run.startedAt === undefined ? {} : { startedAt: run.startedAt },
    ...run.finishedAt === undefined ? {} : { finishedAt: run.finishedAt },
    ...run.finishReason === undefined ? {} : { finishReason: run.finishReason },
    ...run.receipt === undefined ? {} : {
      receipt: {
        status: run.receipt.status,
        summary: run.receipt.summary,
        artifacts: [...run.receipt.artifacts],
        checks: run.receipt.checks.map(check => ({
          name: check.name,
          passed: check.passed,
          ...check.detail === undefined ? {} : { detail: check.detail },
        })),
      },
    },
    ...run.error === undefined ? {} : { error: run.error },
  }
}

/** Shared exact-id/revision parser for every non-create mutation. */
function mutationTarget(args: { readonly id: string; readonly expected_revision: number }): {
  readonly id: string
  readonly expectedRevision: number
} {
  return { id: jobId(args.id), expectedRevision: revision(args.expected_revision) }
}

/** Build a normalized create request without materializing absent defaults. */
function createInput(args: {
  readonly name: string
  readonly expression: string
  readonly prompt: string
  readonly time_zone?: string
  readonly cwd?: string
  readonly agent_preset?: string
  readonly permission_preset?: string
  readonly provider?: string
  readonly model?: string
  readonly misfire_policy?: MisfirePolicy
  readonly max_runtime_seconds?: number
}): CronAdminCreateInput {
  const zone = args.time_zone === undefined ? undefined : timeZone(args.time_zone)
  return {
    name: selector(args.name, 'name', MAX_NAME_LENGTH),
    expression: expression(args.expression, zone),
    prompt: prompt(args.prompt),
    ...zone === undefined ? {} : { timeZone: zone },
    ...args.cwd === undefined ? {} : { cwd: cwd(args.cwd) },
    ...args.agent_preset === undefined ? {} : {
      agentPreset: selector(args.agent_preset, 'agent_preset'),
    },
    ...args.permission_preset === undefined ? {} : {
      permissionPreset: selector(args.permission_preset, 'permission_preset'),
    },
    ...args.provider === undefined ? {} : { provider: selector(args.provider, 'provider') },
    ...args.model === undefined ? {} : { model: selector(args.model, 'model') },
    ...args.misfire_policy === undefined ? {} : { misfirePolicy: args.misfire_policy },
    ...args.max_runtime_seconds === undefined ? {} : {
      maxRuntimeMs: maxRuntimeMs(args.max_runtime_seconds),
    },
  }
}

/** Build a non-empty compare-and-set patch. */
function updateInput(args: {
  readonly expected_revision: number
  readonly confirmation_token?: string
  readonly name?: string
  readonly expression?: string
  readonly time_zone?: string
  readonly prompt?: string
  readonly cwd?: string
  readonly agent_preset?: string
  readonly permission_preset?: string
  readonly provider?: string | null
  readonly model?: string | null
  readonly misfire_policy?: MisfirePolicy
  readonly max_runtime_seconds?: number
}): CronAdminUpdateInput {
  const zone = args.time_zone === undefined ? undefined : timeZone(args.time_zone)
  const fields = Object.keys(args).filter(key =>
    key !== 'expected_revision' && key !== 'confirmation_token')
  if (fields.length === 0) invalid('cron_update requires at least one replacement field')
  return {
    expectedRevision: revision(args.expected_revision),
    ...args.name === undefined ? {} : { name: selector(args.name, 'name', MAX_NAME_LENGTH) },
    ...args.expression === undefined ? {} : {
      expression: expression(args.expression, zone),
    },
    ...zone === undefined ? {} : { timeZone: zone },
    ...args.prompt === undefined ? {} : { prompt: prompt(args.prompt) },
    ...args.cwd === undefined ? {} : { cwd: cwd(args.cwd) },
    ...args.agent_preset === undefined ? {} : {
      agentPreset: selector(args.agent_preset, 'agent_preset'),
    },
    ...args.permission_preset === undefined ? {} : {
      permissionPreset: selector(args.permission_preset, 'permission_preset'),
    },
    ...args.provider === undefined ? {} : {
      provider: args.provider === null ? null : selector(args.provider, 'provider'),
    },
    ...args.model === undefined ? {} : {
      model: args.model === null ? null : selector(args.model, 'model'),
    },
    ...args.misfire_policy === undefined ? {} : { misfirePolicy: args.misfire_policy },
    ...args.max_runtime_seconds === undefined ? {} : {
      maxRuntimeMs: maxRuntimeMs(args.max_runtime_seconds),
    },
  }
}

const CONFIRMATION_PARAMETER = {
  type: 'string',
  description: 'Exact one-time token returned by the first rejected attempt. The same token must '
    + 'also appear in a later direct-human message, and the normalized mutation must be unchanged.',
} as const

const WRITE_NOTICE = 'Every mutation is a two-turn operation: the first valid attempt returns a '
  + 'request-bound confirmation token; only a repeated identical call in a later direct-human '
  + 'top-level turn can execute. Cron-, plugin-, goal-, and subagent-sourced turns are rejected.'

/** Register all eight Cron administration tools in one exact root Agent scope. */
export function registerCronAdminTools(
  rootCtx: Context,
  toolCtx: Context,
  owner: Agent,
  runtime: CronAdminRuntime,
): () => void {
  if (toolCtx.agent !== owner
    || rootCtx.agents.get(owner.id) !== owner
    || !rootCtx.agents.roots().includes(owner)) {
    throw new TypeError('cron administration tools can be registered only on an exact live root Agent context')
  }

  const disposers: Array<() => void> = []

  try {
    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_create',
      description: 'Create one durable Host-level five-field Cron job. The runner receives only '
        + 'deployment-allowlisted tools and cannot administer Cron. ' + WRITE_NOTICE,
      parameters: {
        name: { type: 'string', required: true, description: 'Short human-readable job name.' },
        expression: { type: 'string', required: true, description: 'Five-field cron expression: minute hour day month weekday.' },
        prompt: { type: 'string', required: true, description: 'Task for each fresh occurrence Agent.' },
        time_zone: { type: 'string', description: 'UTC or an IANA time-zone name. Omit for the deployment default.' },
        cwd: { type: 'string', description: 'Absolute working directory. Omit for the deployment default.' },
        agent_preset: { type: 'string', description: 'Configured Agent Preset; omit for the deployment default.' },
        permission_preset: { type: 'string', description: 'Configured permission preset; omit for the deployment default.' },
        provider: { type: 'string', description: 'Optional configured provider selector.' },
        model: { type: 'string', description: 'Optional configured model selector.' },
        misfire_policy: { type: 'string', enum: ['latest', 'skip'], description: 'How restart handles elapsed occurrences.' },
        max_runtime_seconds: { type: 'number', description: `Positive safe integer, at most ${MAX_RUNTIME_SECONDS}.` },
        confirmation_token: CONFIRMATION_PARAMETER,
      },
      output: JOB_OUTPUT,
      async execute(args, exec): Promise<CronJobValue> {
        const execution = cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, [
          'name', 'expression', 'prompt', 'time_zone', 'cwd', 'agent_preset',
          'permission_preset', 'provider', 'model', 'misfire_policy', 'max_runtime_seconds',
          'confirmation_token',
        ])
        const input = createInput(args)
        const authority = requireDirectHumanCronAuthority(
          execution,
          exec.signal,
          'cron_create',
          input,
          args.confirmation_token,
        )
        return jobValue(await runtime.createJob(input, authority))
      },
      presentCall: args => present('Create cron job', 'other', args.name),
    })))

    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_list',
      description: 'List every durable Host-level Cron job visible to this administrator, including exact revision and next run.',
      parameters: {},
      output: JOB_LIST_OUTPUT,
      async execute(args, exec): Promise<CronJobValue[]> {
        cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, [])
        return (await runtime.listJobs()).map(jobValue)
      },
      presentCall: () => present('List cron jobs', 'read'),
    })))

    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_update',
      description: 'Compare-and-set mutable fields on one exact Cron job revision. Omitted fields stay unchanged; null provider/model clears that selector. '
        + WRITE_NOTICE,
      parameters: {
        id: { type: 'string', required: true, description: 'Exact id returned by cron_list.' },
        expected_revision: { type: 'number', required: true, description: 'Exact positive revision returned by cron_list.' },
        name: { type: 'string' },
        expression: { type: 'string', description: 'Replacement five-field cron expression.' },
        prompt: { type: 'string' },
        time_zone: { type: 'string' },
        cwd: { type: 'string' },
        agent_preset: { type: 'string' },
        permission_preset: { type: 'string' },
        provider: { oneOf: [{ type: 'string' }, { type: 'null' }] },
        model: { oneOf: [{ type: 'string' }, { type: 'null' }] },
        misfire_policy: { type: 'string', enum: ['latest', 'skip'] },
        max_runtime_seconds: { type: 'number' },
        confirmation_token: CONFIRMATION_PARAMETER,
      },
      output: JOB_OUTPUT,
      async execute(args, exec): Promise<CronJobValue> {
        const execution = cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, [
          'id', 'expected_revision', 'name', 'expression', 'prompt', 'time_zone', 'cwd',
          'agent_preset', 'permission_preset', 'provider', 'model', 'misfire_policy',
          'max_runtime_seconds', 'confirmation_token',
        ])
        const id = jobId(args.id)
        const input = updateInput(args)
        const authority = requireDirectHumanCronAuthority(
          execution,
          exec.signal,
          'cron_update',
          { id, input },
          args.confirmation_token,
        )
        return jobValue(await runtime.updateJob(id, input, authority))
      },
      presentCall: args => present('Update cron job', 'other', args.id),
    })))

    const registerStateMutation = (
      name: 'cron_pause' | 'cron_resume',
      verb: 'Pause' | 'Resume',
      invoke: (
        id: string,
        expectedRevision: number,
        authority: CronAdminAuthority,
      ) => Promise<CronJob>,
    ): void => {
      disposers.push(toolCtx.tools.register(defineTool({
        name,
        description: `${verb} one exact Cron job revision. ` + WRITE_NOTICE,
        parameters: {
          id: { type: 'string', required: true, description: 'Exact id returned by cron_list.' },
          expected_revision: { type: 'number', required: true, description: 'Exact positive revision returned by cron_list.' },
          confirmation_token: CONFIRMATION_PARAMETER,
        },
        output: JOB_OUTPUT,
        async execute(args, exec): Promise<CronJobValue> {
          const execution = cronAdminExecution(rootCtx, exec, owner)
          requireExactKeys(args, ['id', 'expected_revision', 'confirmation_token'])
          const target = mutationTarget(args)
          const authority = requireDirectHumanCronAuthority(
            execution,
            exec.signal,
            name,
            target,
            args.confirmation_token,
          )
          return jobValue(await invoke(target.id, target.expectedRevision, authority))
        },
        presentCall: args => present(`${verb} cron job`, 'other', args.id),
      })))
    }

    registerStateMutation('cron_pause', 'Pause', (id, expected, authority) =>
      runtime.pauseJob(id, expected, authority))
    registerStateMutation('cron_resume', 'Resume', (id, expected, authority) =>
      runtime.resumeJob(id, expected, authority))

    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_delete',
      description: 'Permanently delete one exact Cron job revision. Existing run-history records remain auditable. '
        + WRITE_NOTICE,
      parameters: {
        id: { type: 'string', required: true, description: 'Exact id returned by cron_list.' },
        expected_revision: { type: 'number', required: true, description: 'Exact positive revision returned by cron_list.' },
        confirmation_token: CONFIRMATION_PARAMETER,
      },
      output: { schema: DELETE_SCHEMA, render: renderCanonicalJson },
      async execute(args, exec): Promise<{ id: string; deleted: boolean }> {
        const execution = cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, ['id', 'expected_revision', 'confirmation_token'])
        const target = mutationTarget(args)
        const authority = requireDirectHumanCronAuthority(
          execution,
          exec.signal,
          'cron_delete',
          target,
          args.confirmation_token,
        )
        return runtime.deleteJob(target.id, target.expectedRevision, authority)
      },
      presentCall: args => present('Delete cron job', 'other', args.id),
    })))

    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_run_now',
      description: 'Claim one manual occurrence of an exact current Cron job revision. The call returns the durable run record, not business success. '
        + WRITE_NOTICE,
      parameters: {
        id: { type: 'string', required: true, description: 'Exact id returned by cron_list.' },
        expected_revision: { type: 'number', required: true, description: 'Exact positive revision returned by cron_list.' },
        confirmation_token: CONFIRMATION_PARAMETER,
      },
      output: RUN_OUTPUT,
      async execute(args, exec): Promise<CronRunValue> {
        const execution = cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, ['id', 'expected_revision', 'confirmation_token'])
        const target = mutationTarget(args)
        const authority = requireDirectHumanCronAuthority(
          execution,
          exec.signal,
          'cron_run_now',
          target,
          args.confirmation_token,
        )
        return runValue(await runtime.runNow(target.id, target.expectedRevision, authority))
      },
      presentCall: args => present('Run cron job now', 'other', args.id),
    })))

    disposers.push(toolCtx.tools.register(defineTool({
      name: 'cron_runs',
      description: 'Read bounded durable Cron run history. Optionally filter by exact job id.',
      parameters: {
        job_id: { type: 'string', description: 'Optional exact job id returned by cron_list.' },
        limit: { type: 'number', description: `Positive safe integer, at most ${MAX_RUN_LIST_LIMIT}; defaults to ${DEFAULT_RUN_LIST_LIMIT}.` },
      },
      output: RUN_LIST_OUTPUT,
      async execute(args, exec): Promise<CronRunValue[]> {
        cronAdminExecution(rootCtx, exec, owner)
        requireExactKeys(args, ['job_id', 'limit'])
        const limit = args.limit ?? DEFAULT_RUN_LIST_LIMIT
        if (!Number.isSafeInteger(limit) || limit < 1 || limit > MAX_RUN_LIST_LIMIT) {
          invalid(`limit must be a positive safe integer no greater than ${MAX_RUN_LIST_LIMIT}`)
        }
        const query: CronRunQuery = {
          limit,
          ...args.job_id === undefined ? {} : { jobId: jobId(args.job_id) },
        }
        return (await runtime.listRuns(query)).map(runValue)
      },
      presentCall: args => present('Read cron runs', 'read', args.job_id),
    })))
  } catch (error: unknown) {
    for (const dispose of disposers.reverse()) dispose()
    throw error
  }

  let active = true
  return () => {
    if (!active) return
    active = false
    for (const dispose of disposers.reverse()) dispose()
  }
}
