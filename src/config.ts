import { isAbsolute, resolve } from 'node:path'
import z from '@deepseek-ai/schemastery'
import { dshHomePath, expandHomePath } from '@deepseek-ai/dsh-home-paths'
import type { MisfirePolicy } from './types.ts'
import { validateTimeZone } from './cron.ts'

const DEFAULT_SHELL_TOOL = process.platform === 'win32' ? 'pwsh' : 'bash'

export const DEFAULT_ALLOWED_TOOLS = [
  DEFAULT_SHELL_TOOL,
  'read',
  'read_image',
  'write',
  'edit',
  'grep',
  'glob',
  'web_search',
  'skill',
] as const

/** User-owned deployment configuration supplied through cordis.yml. */
export interface Config {
  databasePath?: string
  defaultTimeZone?: string
  defaultCwd?: string
  defaultAgentPreset?: string
  defaultPermissionPreset?: string
  defaultMisfirePolicy?: MisfirePolicy
  allowedTools?: string[]
  maxConcurrentRuns?: number
  maxRuntimeMs?: number
  heartbeatIntervalMs?: number
}

/** Schemastery schema used by the Cordis loader. */
export const Config: z<Config> = z.object({
  databasePath: z.string().default(dshHomePath('cron', 'cron.sqlite')),
  defaultTimeZone: z.string().default('UTC'),
  defaultCwd: z.string(),
  defaultAgentPreset: z.string().default('standard'),
  defaultPermissionPreset: z.string().default('workspace-write'),
  defaultMisfirePolicy: z.union(['latest', 'skip']).default('latest'),
  allowedTools: z.array(z.string()).default([...DEFAULT_ALLOWED_TOOLS]),
  maxConcurrentRuns: z.number().step(1).min(1).max(16).default(1),
  maxRuntimeMs: z.number().step(1).min(1_000).default(3_600_000),
  heartbeatIntervalMs: z.number().step(1).min(1_000).default(30_000),
})

/** Fully normalized immutable policy consumed by runtime and tools. */
export interface ResolvedConfig {
  readonly databasePath: string
  readonly defaultTimeZone: string
  readonly defaultCwd?: string
  readonly defaultAgentPreset: string
  readonly defaultPermissionPreset: string
  readonly defaultMisfirePolicy: MisfirePolicy
  readonly allowedTools: readonly string[]
  readonly maxConcurrentRuns: number
  readonly maxRuntimeMs: number
  readonly heartbeatIntervalMs: number
}

function requiredText(value: string | undefined, fallback: string, field: string): string {
  const normalized = (value ?? fallback).trim()
  if (normalized.length === 0) throw new TypeError(`${field} must be non-empty`)
  return normalized
}

function positiveInteger(value: number | undefined, fallback: number, field: string, max?: number): number {
  const resolved = value ?? fallback
  if (!Number.isSafeInteger(resolved) || resolved < 1 || (max !== undefined && resolved > max)) {
    throw new TypeError(`${field} must be a positive safe integer${max === undefined ? '' : ` no greater than ${String(max)}`}`)
  }
  return resolved
}

function normalizePath(value: string, field: string): string {
  const expanded = expandHomePath(value.trim())
  if (expanded.length === 0) throw new TypeError(`${field} must be non-empty`)
  return isAbsolute(expanded) ? resolve(expanded) : resolve(process.cwd(), expanded)
}

function assertTimeZone(timeZone: string): void {
  try {
    validateTimeZone(timeZone)
  } catch (error: unknown) {
    throw new TypeError(`defaultTimeZone must be a valid IANA time zone: ${timeZone}`, { cause: error })
  }
}

/** Validate again for direct apply() calls that bypass Loader schema normalization. */
export function resolveConfig(config: Config = {}): ResolvedConfig {
  const defaultTimeZone = requiredText(config.defaultTimeZone, 'UTC', 'defaultTimeZone')
  assertTimeZone(defaultTimeZone)

  const allowedTools = [...new Set((config.allowedTools ?? [...DEFAULT_ALLOWED_TOOLS]).map(name => name.trim()))]
  if (allowedTools.length === 0 || allowedTools.some(name => name.length === 0)) {
    throw new TypeError('allowedTools must contain at least one non-empty tool name')
  }
  const reserved = allowedTools.filter(name => name === 'run_code' || name === 'cron_complete' || name.startsWith('cron_'))
  if (reserved.length > 0) {
    throw new TypeError(`allowedTools contains reserved Cron/control tool names: ${reserved.join(', ')}`)
  }

  const defaultMisfirePolicy = config.defaultMisfirePolicy ?? 'latest'
  if (defaultMisfirePolicy !== 'latest' && defaultMisfirePolicy !== 'skip') {
    throw new TypeError('defaultMisfirePolicy must be latest or skip')
  }

  return {
    databasePath: normalizePath(config.databasePath ?? dshHomePath('cron', 'cron.sqlite'), 'databasePath'),
    defaultTimeZone,
    ...config.defaultCwd === undefined ? {} : { defaultCwd: normalizePath(config.defaultCwd, 'defaultCwd') },
    defaultAgentPreset: requiredText(config.defaultAgentPreset, 'standard', 'defaultAgentPreset'),
    defaultPermissionPreset: requiredText(
      config.defaultPermissionPreset,
      'workspace-write',
      'defaultPermissionPreset',
    ),
    defaultMisfirePolicy,
    allowedTools,
    maxConcurrentRuns: positiveInteger(config.maxConcurrentRuns, 1, 'maxConcurrentRuns', 16),
    maxRuntimeMs: positiveInteger(config.maxRuntimeMs, 3_600_000, 'maxRuntimeMs'),
    heartbeatIntervalMs: positiveInteger(config.heartbeatIntervalMs, 30_000, 'heartbeatIntervalMs'),
  }
}
