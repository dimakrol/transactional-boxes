#!/usr/bin/env node
'use strict';

/**
 * Load-test script for transactions-service.
 *
 * Usage:
 *   node scripts/load-test.js <usersCount> <txPerUser> [options]
 *
 * Options:
 *   --base-url=<url>       transactions-service base URL (default: http://localhost:3000)
 *   --concurrency=<n>      number of requests in flight at once (default: 20)
 *   --min-amount=<n>       minimum transaction amount (default: -500)
 *   --max-amount=<n>       maximum transaction amount (default: 500)
 *   --no-verify            skip post-run balance verification against the databases
 *
 * Sends usersCount * txPerUser transactions with random amounts, in random order,
 * to POST /transactions. After sending, optionally verifies (via `docker exec` + psql)
 * that the balances in both databases match the sum of amounts sent per user.
 */

const { randomUUID } = require('node:crypto');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const execFileAsync = promisify(execFile);

function parseArgs(argv) {
  const positional = [];
  const flags = {};
  for (const arg of argv) {
    if (arg.startsWith('--')) {
      const [key, value] = arg.slice(2).split('=');
      flags[key] = value === undefined ? true : value;
    } else {
      positional.push(arg);
    }
  }
  return { positional, flags };
}

function printUsageAndExit() {
  console.error(
    'Usage: node scripts/load-test.js <usersCount> <txPerUser> ' +
      '[--base-url=http://localhost:3000] [--concurrency=20] ' +
      '[--min-amount=-500] [--max-amount=500] [--no-verify]',
  );
  process.exit(1);
}

function randomAmount(min, max) {
  const value = Math.random() * (max - min) + min;
  return value.toFixed(2);
}

function shuffle(array) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

async function runPool(items, concurrency, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runNext() {
    while (true) {
      const current = nextIndex++;
      if (current >= items.length) return;
      results[current] = await worker(items[current], current);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, runNext);
  await Promise.all(workers);
  return results;
}

async function sendTransaction(baseUrl, tx) {
  const startedAt = Date.now();
  try {
    const res = await fetch(`${baseUrl}/transactions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        idempotency_id: tx.idempotencyId,
        user_id: tx.userId,
        amount: tx.amount,
      }),
    });
    const durationMs = Date.now() - startedAt;
    if (!res.ok) {
      const body = await res.text().catch(() => '');
      return { ok: false, status: res.status, durationMs, body };
    }
    return { ok: true, status: res.status, durationMs };
  } catch (err) {
    return { ok: false, status: 0, durationMs: Date.now() - startedAt, error: err.message };
  }
}

async function fetchBalances(container, dbUser, dbName, userIds) {
  if (userIds.length === 0) return new Map();
  const idList = userIds.map((id) => `'${id}'`).join(',');
  const query = `SELECT id, balance FROM users WHERE id IN (${idList});`;
  const { stdout } = await execFileAsync('docker', [
    'exec',
    container,
    'psql',
    '-U',
    dbUser,
    '-d',
    dbName,
    '-t',
    '-A',
    '-F',
    ',',
    '-c',
    query,
  ]);
  const map = new Map();
  for (const line of stdout.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const [id, balance] = trimmed.split(',');
    map.set(id, Number(balance));
  }
  return map;
}

async function main() {
  const { positional, flags } = parseArgs(process.argv.slice(2));
  const usersCount = Number(positional[0]);
  const txPerUser = Number(positional[1]);

  if (!Number.isInteger(usersCount) || usersCount <= 0 || !Number.isInteger(txPerUser) || txPerUser <= 0) {
    printUsageAndExit();
  }

  const baseUrl = flags['base-url'] || 'http://localhost:3000';
  const concurrency = Number(flags.concurrency) > 0 ? Number(flags.concurrency) : 20;
  const minAmount = flags['min-amount'] !== undefined ? Number(flags['min-amount']) : -500;
  const maxAmount = flags['max-amount'] !== undefined ? Number(flags['max-amount']) : 500;
  const verify = !flags['no-verify'];

  const runId = Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
  const userIds = Array.from({ length: usersCount }, (_, i) => `loadtest-${runId}-u${i + 1}`);

  const transactions = [];
  const expectedBalance = new Map(userIds.map((id) => [id, 0]));
  for (const userId of userIds) {
    for (let i = 0; i < txPerUser; i++) {
      const amount = randomAmount(minAmount, maxAmount);
      transactions.push({ userId, amount, idempotencyId: randomUUID() });
      expectedBalance.set(userId, expectedBalance.get(userId) + Number(amount));
    }
  }
  shuffle(transactions);

  console.log(
    `Sending ${transactions.length} transactions for ${usersCount} users ` +
      `(${txPerUser} each) to ${baseUrl} with concurrency=${concurrency}...`,
  );

  const startedAt = Date.now();
  const results = await runPool(transactions, concurrency, (tx) => sendTransaction(baseUrl, tx));
  const elapsedMs = Date.now() - startedAt;

  const succeeded = results.filter((r) => r.ok);
  const failed = results.filter((r) => !r.ok);
  const avgLatency = results.reduce((sum, r) => sum + r.durationMs, 0) / results.length;

  console.log('\n--- Send summary ---');
  console.log(`Total:      ${results.length}`);
  console.log(`Succeeded:  ${succeeded.length}`);
  console.log(`Failed:     ${failed.length}`);
  console.log(`Elapsed:    ${(elapsedMs / 1000).toFixed(2)}s (${(results.length / (elapsedMs / 1000)).toFixed(1)} req/s)`);
  console.log(`Avg latency: ${avgLatency.toFixed(1)}ms`);

  if (failed.length > 0) {
    const byStatus = new Map();
    for (const r of failed) {
      const key = r.status || r.error || 'unknown';
      byStatus.set(key, (byStatus.get(key) || 0) + 1);
    }
    console.log('Failure breakdown:');
    for (const [key, count] of byStatus) {
      console.log(`  ${key}: ${count}`);
    }
  }

  if (!verify) {
    console.log('\nSkipping verification (--no-verify).');
    return;
  }

  console.log('\nWaiting for outbox -> Kafka -> inbox propagation...');
  const waitMs = Math.min(15000, 3000 + transactions.length * 20);
  await new Promise((resolve) => setTimeout(resolve, waitMs));

  console.log('Verifying balances against both databases...');
  let serviceABalances;
  let serviceBBalances;
  try {
    [serviceABalances, serviceBBalances] = await Promise.all([
      fetchBalances('transactional-boxes-pg-a-1', 'service_a', 'service_a', userIds),
      fetchBalances('transactional-boxes-pg-b-1', 'service_b', 'service_b', userIds),
    ]);
  } catch (err) {
    console.warn(`Could not verify via docker exec (is the stack running locally?): ${err.message}`);
    return;
  }

  let mismatches = 0;
  for (const userId of userIds) {
    const expected = Number(expectedBalance.get(userId).toFixed(8));
    const a = serviceABalances.get(userId);
    const b = serviceBBalances.get(userId);
    const aOk = a !== undefined && Math.abs(a - expected) < 1e-6;
    const bOk = b !== undefined && Math.abs(b - expected) < 1e-6;
    if (!aOk || !bOk) {
      mismatches++;
      console.log(`  MISMATCH ${userId}: expected=${expected} service-a=${a} service-b=${b}`);
    }
  }

  if (mismatches === 0) {
    console.log(`All ${userIds.length} user balances match on both services.`);
  } else {
    console.log(`${mismatches} of ${userIds.length} user balances did not match (see above).`);
    console.log('Note: inbox/outbox sync can lag under heavy load — rerun verification or wait longer if this persists.');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
