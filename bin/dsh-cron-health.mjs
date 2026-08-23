#!/usr/bin/env node

import { existsSync } from 'node:fs'
import { homedir } from 'node:os'
import { join, resolve } from 'node:path'
import { DatabaseSync } from 'node:sqlite'

const defaultHome = process.env.DSH_HOME?.trim() || join(homedir(), '.dsh')
const databasePath = resolve(process.argv[2] ?? join(defaultHome, 'cron', 'cron.sqlite'))
const maxAgeSeconds = Number(process.argv[3] ?? '90')

if (!Number.isFinite(maxAgeSeconds) || maxAgeSeconds <= 0) {
  process.stderr.write('max age must be a positive number of seconds\n')
  process.exit(2)
}

if (!existsSync(databasePath)) {
  process.stderr.write(`cron database not found: ${databasePath}\n`)
  process.exit(1)
}

const database = new DatabaseSync(databasePath, { readOnly: true })
try {
  const row = database.prepare(
    "SELECT value_json AS valueJson FROM cron_meta WHERE key = 'heartbeat'",
  ).get()
  if (row === undefined || typeof row.valueJson !== 'string') {
    process.stderr.write('cron heartbeat not found\n')
    process.exitCode = 1
  } else {
    const heartbeat = JSON.parse(row.valueJson)
    const heartbeatAt = Date.parse(heartbeat.heartbeatAt)
    const ageSeconds = (Date.now() - heartbeatAt) / 1000
    if (!Number.isFinite(heartbeatAt) || ageSeconds < 0 || ageSeconds > maxAgeSeconds) {
      process.stderr.write(`cron heartbeat stale: ${String(ageSeconds)} seconds\n`)
      process.exitCode = 1
    } else {
      process.stdout.write(`${JSON.stringify({ ...heartbeat, ageSeconds })}\n`)
    }
  }
} finally {
  database.close()
}
