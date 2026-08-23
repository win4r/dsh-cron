import { describe, expect, it } from 'vitest'
import { resolveConfig } from '../src/config.ts'

describe('resolveConfig', () => {
  it('materializes safe defaults', () => {
    const config = resolveConfig({ databasePath: './cron.sqlite' })
    expect(config.databasePath).toMatch(/cron\.sqlite$/)
    expect(config.defaultTimeZone).toBe('UTC')
    expect(config.defaultMisfirePolicy).toBe('latest')
    expect(config.maxConcurrentRuns).toBe(1)
    expect(config.allowedTools).toContain('read')
    expect(config.allowedTools).toContain(process.platform === 'win32' ? 'pwsh' : 'bash')
    expect(config.allowedTools).not.toContain('web_fetch')
  })

  it('rejects an invalid time zone', () => {
    expect(() => resolveConfig({ defaultTimeZone: 'Mars/Olympus' })).toThrow(/IANA time zone/)
    expect(() => resolveConfig({ defaultTimeZone: 'PST' })).toThrow(/IANA time zone/)
  })

  it('rejects Cron control capabilities in the runner allowlist', () => {
    expect(() => resolveConfig({ allowedTools: ['read', 'cron_create'] })).toThrow(/reserved/)
  })
})
