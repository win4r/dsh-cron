import { describe, expect, test } from 'vitest'
import {
  deterministicRunId,
  deterministicSessionId,
  latestOccurrence,
  nextOccurrence,
  validateCronExpression,
  validateTimeZone,
} from '../src/cron.js'

describe('five-field cron calculations', () => {
  test('accepts an exact five-field expression and rejects other arities', () => {
    expect(() => validateCronExpression('*/15 9-17 * * 1-5')).not.toThrow()
    expect(() => validateCronExpression('  */15   9-17 * * 1-5  ')).not.toThrow()
    expect(() => validateCronExpression('')).toThrow(/must not be empty/)
    expect(() => validateCronExpression('@daily')).toThrow(/exactly 5 fields/)
    expect(() => validateCronExpression('0 0 9 * * 1-5')).toThrow(/exactly 5 fields/)
    expect(() => validateCronExpression('61 9 * * *')).toThrow()
  })

  test('uses cron-parser strict ambiguity checks', () => {
    expect(() => validateCronExpression('0 9 1 * 1')).toThrow(/dayOfMonth and dayOfWeek/)
  })

  test('requires an IANA zone, not a fixed offset or abbreviation', () => {
    expect(() => validateTimeZone('America/Los_Angeles')).not.toThrow()
    expect(() => validateTimeZone('Etc/GMT+1')).not.toThrow()
    expect(() => validateTimeZone('UTC')).not.toThrow()
    expect(() => validateTimeZone('PST')).toThrow(/IANA/)
    expect(() => validateTimeZone('+01:00')).toThrow(/IANA/)
    expect(() => validateTimeZone('Mars/Olympus')).toThrow(/unknown IANA/)
  })

  test('returns the first occurrence strictly after the cursor', () => {
    expect(nextOccurrence(
      '0 9 * * 1-5',
      'America/Los_Angeles',
      '2026-08-21T16:00:00.000Z',
    )).toBe('2026-08-24T16:00:00.000Z')
  })

  test('delegates DST gap and fold handling to cron-parser', () => {
    // 02:30 does not exist on the spring-forward day; cron-parser advances it
    // into the first matching post-transition wall-clock period.
    expect(nextOccurrence(
      '30 2 * * *',
      'America/Los_Angeles',
      '2026-03-07T10:30:00.000Z',
    )).toBe('2026-03-08T10:30:00.000Z')

    // A repeated 01:30 during fall-back is admitted once, not twice.
    const first = nextOccurrence(
      '30 1 * * *',
      'America/Los_Angeles',
      '2026-10-31T08:30:00.000Z',
    )
    expect(first).toBe('2026-11-01T08:30:00.000Z')
    expect(nextOccurrence('30 1 * * *', 'America/Los_Angeles', first))
      .toBe('2026-11-02T09:30:00.000Z')
  })

  test('selects the latest misfired occurrence without crossing nextRunAt', () => {
    expect(latestOccurrence(
      '* * * * *',
      'UTC',
      '2020-01-01T00:00:00.000Z',
      '2026-08-22T18:14:37.500Z',
    )).toBe('2026-08-22T18:14:00.000Z')

    // Lower bound is inclusive because it is the durable first pending run.
    expect(latestOccurrence(
      '0 9 * * *',
      'UTC',
      '2026-08-22T09:00:00.000Z',
      '2026-08-22T09:00:00.000Z',
    )).toBe('2026-08-22T09:00:00.000Z')

    expect(latestOccurrence(
      '0 9 * * *',
      'UTC',
      '2026-08-23T09:00:00.000Z',
      '2026-08-22T09:00:00.000Z',
    )).toBeUndefined()
  })

  test('keeps backward misfire selection on cron-parser forward DST sequences', () => {
    expect(latestOccurrence(
      '30 2 * * *',
      'America/Los_Angeles',
      '2026-03-07T10:30:00.000Z',
      '2026-03-08T10:34:00.000Z',
    )).toBe('2026-03-08T10:30:00.000Z')

    const firstFold = '2026-11-01T08:30:00.000Z'
    expect(nextOccurrence(
      '30 1 * * *',
      'America/Los_Angeles',
      '2026-10-31T08:30:00.000Z',
    )).toBe(firstFold)

    // cron-parser prev() alone returns the second 01:30 (09:30Z), although
    // repeated next() from the durable first occurrence skips that duplicate.
    expect(latestOccurrence(
      '30 1 * * *',
      'America/Los_Angeles',
      firstFold,
      '2026-11-01T09:45:00.000Z',
    )).toBe(firstFold)
    expect(nextOccurrence('30 1 * * *', 'America/Los_Angeles', firstFold))
      .toBe('2026-11-02T09:30:00.000Z')

    // A Job created during the repeated hour owns its durable second-fold
    // lower bound; alignment must never move behind that boundary.
    const secondFold = nextOccurrence(
      '30 1 * * *',
      'America/Los_Angeles',
      '2026-11-01T09:00:00.000Z',
    )
    expect(secondFold).toBe('2026-11-01T09:30:00.000Z')
    expect(latestOccurrence(
      '30 1 * * *',
      'America/Los_Angeles',
      secondFold,
      '2026-11-01T09:45:00.000Z',
    )).toBe(secondFold)

    // For an every-minute expression cron-parser's forward sequence does
    // traverse the repeated hour, so the second-fold occurrence remains valid.
    expect(latestOccurrence(
      '* * * * *',
      'America/Los_Angeles',
      '2026-11-01T08:59:00.000Z',
      '2026-11-01T09:45:30.000Z',
    )).toBe('2026-11-01T09:45:00.000Z')

    const lordHoweZone = 'Australia/Lord_Howe'
    expect(nextOccurrence(
      '0 * * * *',
      lordHoweZone,
      '2026-04-04T14:00:00.000Z',
    )).toBe('2026-04-04T15:30:00.000Z')

    const lordHoweFirstFold = nextOccurrence(
      '45 1 * * *',
      lordHoweZone,
      '2026-04-04T13:00:00.000Z',
    )
    expect(lordHoweFirstFold).toBe('2026-04-04T14:45:00.000Z')
    expect(latestOccurrence(
      '45 1 * * *',
      lordHoweZone,
      lordHoweFirstFold,
      '2026-04-04T15:00:00.000Z',
    )).toBe(lordHoweFirstFold)
    expect(latestOccurrence(
      '45 1 * * *',
      lordHoweZone,
      lordHoweFirstFold,
      '2026-04-04T15:20:00.000Z',
    )).toBe(lordHoweFirstFold)

    const lordHoweSecondFold = nextOccurrence(
      '45 1 * * *',
      lordHoweZone,
      '2026-04-04T15:00:00.000Z',
    )
    expect(lordHoweSecondFold).toBe('2026-04-04T15:15:00.000Z')
    expect(latestOccurrence(
      '45 1 * * *',
      lordHoweZone,
      lordHoweSecondFold,
      '2026-04-04T15:20:00.000Z',
    )).toBe(lordHoweSecondFold)
  })

  test('produces stable trigger-scoped run and session ids', () => {
    const scheduledAt = '2026-08-22T18:14:00.000Z'
    const run = deterministicRunId('daily-report', scheduledAt)
    expect(run).toBe(deterministicRunId('daily-report', scheduledAt, 'schedule'))
    expect(run).not.toBe(deterministicRunId('daily-report', scheduledAt, 'manual'))
    expect(deterministicSessionId('daily-report', scheduledAt))
      .toMatch(/^cron-session-[a-f0-9]{32}$/u)
  })
})
