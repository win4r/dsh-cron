/** Host-level durable Cron plugin for DeepSeek Harness. */

import type { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { Config as ConfigSchema, resolveConfig, type Config as PluginConfig } from './config.ts'
import { CronRuntime } from './runtime.ts'
import { CronStore } from './store.ts'
import { registerCronAdminTools } from './tools.ts'

/** Stable Cordis plugin id. */
export const name = 'cron'

/** Host services required for durable recovery and fresh occurrence Agents. */
export const inject = [
  'agentDefaultModel',
  'agentPresets',
  'permissionPresets',
  'agents',
  'sessions',
  'sessionPersistence',
  'tools',
]

export const Config = ConfigSchema
export type Config = PluginConfig

/** Mount one Host scheduler and root-Agent-scoped administration surface. */
export function apply(ctx: Context, config: PluginConfig = {}): () => Promise<void> {
  const resolved = resolveConfig(config)
  const store = new CronStore(resolved.databasePath)
  const runtime = new CronRuntime(ctx, store, resolved)
  const adminTools = new Map<Agent, () => void>()

  /** Install management tools only on a live human-facing runtime root. */
  const attachAdmin = (agent: Agent): void => {
    if (runtime.isRunnerAgent(agent)
      || adminTools.has(agent)
      || ctx.agents.get(agent.id) !== agent
      || !ctx.agents.roots().includes(agent)) return
    try {
      adminTools.set(agent, registerCronAdminTools(ctx, agent.ctx, agent, runtime))
    } catch (error: unknown) {
      ctx.logger.warn(`cron: could not attach admin tools to root ${agent.id}: ${error instanceof Error ? error.message : String(error)}`)
    }
  }

  let disposeCreated: (() => void) | undefined
  let disposeRemoved: (() => void) | undefined
  try {
    disposeCreated = ctx.on('agent/created', ({ agent }) => { attachAdmin(agent) })
    disposeRemoved = ctx.on('agent/disposed', ({ agent }) => {
      adminTools.get(agent)?.()
      adminTools.delete(agent)
    })
    // Hot-loading the Host plugin should not require recreating a human root.
    for (const agent of ctx.agents.roots()) attachAdmin(agent)
    void runtime.start()
  } catch (error: unknown) {
    disposeRemoved?.()
    disposeCreated?.()
    for (const dispose of [...adminTools.values()].reverse()) dispose()
    store.close()
    throw error
  }

  let active = true
  return async () => {
    if (!active) return
    active = false
    disposeRemoved?.()
    disposeCreated?.()
    for (const dispose of [...adminTools.values()].reverse()) dispose()
    adminTools.clear()
    try {
      await runtime.dispose()
    } finally {
      store.close()
    }
  }
}

export * from './completion.ts'
export * from './cron.ts'
export * from './runtime.ts'
export * from './store.ts'
export * from './types.ts'
export { resolveConfig }
