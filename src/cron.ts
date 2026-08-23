import { createHash } from 'node:crypto'
import { CronExpressionParser } from 'cron-parser'
import type { RunTrigger } from './types.js'

const FIVE_FIELD_COUNT = 5
const IANA_ZONE_WITH_AREA = /^[A-Za-z0-9._+-]+(?:\/[A-Za-z0-9._+-]+)+$/u
const MINUTE_MS = 60_000
const FOLD_OFFSET_SAMPLE_MS = 12 * 60 * MINUTE_MS
const DST_ALIGNMENT_STEP_LIMIT = 2 * 24 * 60 + 2

const wallClockFormatters = new Map<string, Intl.DateTimeFormat>()

function wallClock(timeZone: string, date: Date): {
  readonly key: string
  readonly offsetMinutes: number
} {
  let formatter = wallClockFormatters.get(timeZone)
  if (formatter === undefined) {
    formatter = new Intl.DateTimeFormat('en-US-u-nu-latn', {
      timeZone,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hourCycle: 'h23',
    })
    wallClockFormatters.set(timeZone, formatter)
  }
  const values = Object.fromEntries(formatter.formatToParts(date)
    .filter(part => part.type !== 'literal')
    .map(part => [part.type, part.value]))
  const year = Number(values.year)
  const month = Number(values.month)
  const day = Number(values.day)
  const hour = Number(values.hour)
  const minute = Number(values.minute)
  const second = Number(values.second)
  const wallMillis = Date.UTC(year, month - 1, day, hour, minute, second)
  const instantMillis = Math.floor(date.getTime() / 1_000) * 1_000
  return {
    key: `${values.year}-${values.month}-${values.day}T${values.hour}:${values.minute}:${values.second}`,
    offsetMinutes: (wallMillis - instantMillis) / MINUTE_MS,
  }
}

/**
 * cron-parser's backward iterator can enter the second copy of a folded local
 * time even when its forward iterator skips that copy for this expression.
 * Return the earlier offset only when `candidate` is specifically the later
 * copy of an ambiguous wall-clock instant.
 */
function earlierFoldOffset(timeZone: string, candidate: Date): number | undefined {
  if (timeZone === 'UTC') return undefined
  const current = wallClock(timeZone, candidate)
  const samples = [
    new Date(candidate.getTime() - FOLD_OFFSET_SAMPLE_MS),
    new Date(candidate.getTime() + FOLD_OFFSET_SAMPLE_MS),
  ]
  for (const sample of samples) {
    const otherOffset = wallClock(timeZone, sample).offsetMinutes
    if (otherOffset <= current.offsetMinutes) continue
    const alternative = new Date(
      candidate.getTime() + (current.offsetMinutes - otherOffset) * MINUTE_MS,
    )
    const alternativeWall = wallClock(timeZone, alternative)
    if (alternativeWall.offsetMinutes === otherOffset && alternativeWall.key === current.key) {
      return otherOffset
    }
  }
  return undefined
}

/** Align a backward candidate with the sequence produced by repeated next(). */
function alignFoldCandidate(
  expression: string,
  timeZone: string,
  candidate: Date,
): Date {
  const earlierOffset = earlierFoldOffset(timeZone, candidate)
  if (earlierOffset === undefined) return candidate

  const original = candidate
  let current = candidate
  const laterOffset = wallClock(timeZone, candidate).offsetMinutes
  // Five-field Cron cannot produce more than one occurrence per minute. The
  // offset delta therefore bounds the backward chain through the repeated span.
  const maxSteps = Math.abs(earlierOffset - laterOffset) + 2
  for (let step = 0; step < maxSteps; step += 1) {
    const previous = parse(expression, timeZone, current).prev().toDate()
    const forward = strictlyNextDate(expression, timeZone, previous)
    if (forward.getTime() !== current.getTime()) return previous
    if (wallClock(timeZone, previous).offsetMinutes !== wallClock(timeZone, current).offsetMinutes) {
      return original
    }
    current = previous
  }
  return original
}

function instant(value: string | Date, label: string): Date {
  const date = value instanceof Date ? new Date(value.getTime()) : new Date(value)
  if (!Number.isFinite(date.getTime())) {
    throw new TypeError(`${label} must be a valid instant`)
  }
  return date
}

function normalizedExpression(expression: string): string {
  if (typeof expression !== 'string') {
    throw new TypeError('cron expression must be a string')
  }

  const normalized = expression.trim().replace(/\s+/gu, ' ')
  if (normalized === '') {
    throw new TypeError('cron expression must not be empty')
  }

  const fields = normalized.split(' ')
  if (fields.length !== FIVE_FIELD_COUNT) {
    throw new TypeError(`cron expression must contain exactly ${FIVE_FIELD_COUNT} fields`)
  }
  return normalized
}

/**
 * Assert that a time-zone name is an IANA zone (rather than a locale-specific
 * abbreviation or a fixed numeric offset). IANA aliases containing a slash are
 * intentionally accepted because ICU canonicalizes them for cron-parser.
 */
export function validateTimeZone(timeZone: string): void {
  if (typeof timeZone !== 'string' || timeZone.trim() !== timeZone || timeZone === '') {
    throw new TypeError('timeZone must be a non-empty IANA time-zone name')
  }
  if (timeZone !== 'UTC' && !IANA_ZONE_WITH_AREA.test(timeZone)) {
    throw new TypeError(`timeZone must be an IANA name, received ${JSON.stringify(timeZone)}`)
  }

  try {
    // Construction performs ICU's authoritative zone-name validation without
    // depending on the Host process's current locale or time zone.
    new Intl.DateTimeFormat('en-US', { timeZone }).format(0)
  } catch {
    throw new TypeError(`unknown IANA time zone: ${timeZone}`)
  }
}

function parse(
  expression: string,
  timeZone: string,
  currentDate: Date,
) {
  const normalized = normalizedExpression(expression)
  validateTimeZone(timeZone)

  // cron-parser's strict mode requires all six fields. Prefixing a fixed zero
  // second preserves a strict five-field public contract while retaining its
  // ambiguity and range validation. hashSeed makes H expressions deterministic
  // across Host restarts.
  return CronExpressionParser.parse(`0 ${normalized}`, {
    currentDate,
    hashSeed: createHash('sha256').update(normalized).digest('hex'),
    strict: true,
    tz: timeZone,
  })
}

/**
 * Produce a strictly increasing next occurrence despite cron-parser's known
 * non-hour fold edge where next() may throw or briefly move backwards from an
 * exact occurrence (for example Australia/Lord_Howe's 30-minute rollback).
 */
function strictlyNextDate(expression: string, timeZone: string, cursor: Date): Date {
  let lastError: unknown
  for (let step = 0; step < DST_ALIGNMENT_STEP_LIMIT; step += 1) {
    const probeOffset = step === 0 ? 0 : step === 1 ? 1 : (step - 1) * MINUTE_MS
    const probe = new Date(cursor.getTime() + probeOffset)
    if (!Number.isFinite(probe.getTime())) break
    try {
      const candidate = parse(expression, timeZone, probe).next().toDate()
      if (candidate.getTime() > cursor.getTime()) return candidate
    } catch (error: unknown) {
      lastError = error
    }
  }
  throw new Error('cron-parser could not produce a strictly increasing occurrence', {
    ...lastError === undefined ? {} : { cause: lastError },
  })
}

/** Validate an exact five-field calendar expression and its ranges. */
export function validateCronExpression(expression: string): void {
  parse(expression, 'UTC', new Date(0))
}

/** Return the first occurrence strictly after `after`, as a canonical UTC ISO instant. */
export function nextOccurrence(
  expression: string,
  timeZone: string,
  after: string | Date,
): string {
  const cursor = instant(after, 'after')
  return strictlyNextDate(expression, timeZone, cursor).toISOString()
}

/**
 * Return the latest occurrence in the inclusive interval
 * `[notBefore, through]`. The inclusive lower bound is deliberate: callers pass
 * a job's durable `nextRunAt`, which is itself the first pending occurrence.
 * Searching backwards from `through` keeps recovery constant-time even after a
 * long Host outage.
 */
export function latestOccurrence(
  expression: string,
  timeZone: string,
  notBefore: string | Date,
  through: string | Date,
): string | undefined {
  const lower = instant(notBefore, 'notBefore')
  const upper = instant(through, 'through')
  if (lower.getTime() > upper.getTime()) return undefined

  // prev() is strictly-before, so advance one millisecond to include an
  // occurrence exactly equal to `through`.
  const inclusiveCursor = new Date(upper.getTime() + 1)
  if (!Number.isFinite(inclusiveCursor.getTime())) {
    throw new RangeError('through is too large to calculate an occurrence')
  }
  const backward = parse(expression, timeZone, inclusiveCursor).prev().toDate()
  const aligned = alignFoldCandidate(expression, timeZone, backward)
  // `nextRunAt` is the durable start of this Job's own forward sequence. A Job
  // created inside the second fold can legitimately begin there even when a
  // sequence that started before the fold would have skipped that copy.
  // The lower bound is not an arbitrary search cursor: it is the exact
  // occurrence durably stored in nextRunAt. Some non-hour DST folds make
  // cron-parser's backward iterator jump behind that occurrence entirely, so
  // it is also the authoritative forward-sequence anchor.
  let candidate = aligned.getTime() < lower.getTime()
    || backward.getTime() < lower.getTime()
    ? lower
    : aligned

  // The backward iterator can also omit a forward-only occurrence synthesized
  // for a spring DST gap (for example, 02:30 becoming 03:30). Walk forward
  // from the near-upper backward candidate until the next occurrence would
  // cross `through`. Under ordinary dates this loop executes zero times; the
  // explicit bound contains any future parser/ICU disagreement.
  for (let step = 0; step < DST_ALIGNMENT_STEP_LIMIT; step += 1) {
    const next = strictlyNextDate(expression, timeZone, candidate)
    if (next.getTime() > upper.getTime()) return candidate.toISOString()
    if (next.getTime() <= candidate.getTime()) {
      throw new Error('cron-parser returned a non-increasing forward occurrence')
    }
    candidate = next
  }
  throw new Error('cron-parser DST alignment exceeded its bounded forward scan')
}

function occurrenceDigest(jobId: string, scheduledAt: string, trigger: RunTrigger): string {
  if (jobId === '') throw new TypeError('jobId must not be empty')
  const canonicalScheduledAt = instant(scheduledAt, 'scheduledAt').toISOString()
  return createHash('sha256')
    .update(trigger)
    .update('\0')
    .update(jobId)
    .update('\0')
    .update(canonicalScheduledAt)
    .digest('hex')
    .slice(0, 32)
}

/** Stable idempotency key for one exact occurrence. */
export function deterministicRunId(
  jobId: string,
  scheduledAt: string,
  trigger: RunTrigger = 'schedule',
): string {
  return `cron-run-${occurrenceDigest(jobId, scheduledAt, trigger)}`
}

/** Stable Session id paired one-to-one with an occurrence. */
export function deterministicSessionId(
  jobId: string,
  scheduledAt: string,
  trigger: RunTrigger = 'schedule',
): string {
  return `cron-session-${occurrenceDigest(jobId, scheduledAt, trigger)}`
}
