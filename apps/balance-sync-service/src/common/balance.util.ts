import { and, eq } from 'drizzle-orm';
import Decimal from 'decimal.js';
import { users } from '../db/schema';
import type { Db } from '../db/db.module';

export class BalanceUpdateConflictError extends Error {
  constructor(userId: string, attempts: number) {
    super(`Could not update balance for user ${userId} after ${attempts} attempts (concurrent updates)`);
  }
}

/**
 * Applies `delta` to a user's balance inside the given (already open) transaction.
 * Auto-creates the user with balance=0 if missing, then optimistically
 * retries the CAS update up to `maxAttempts` times on version conflicts.
 */
export async function applyBalanceDelta(
  tx: Db,
  userId: string,
  delta: string,
  maxAttempts: number,
): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const inserted = await tx
      .insert(users)
      .values({ id: userId, balance: '0', version: 0 })
      .onConflictDoNothing({ target: users.id })
      .returning();

    const current =
      inserted[0] ?? (await tx.select().from(users).where(eq(users.id, userId)).limit(1))[0];

    if (!current) {
      throw new Error(`Failed to read or create user ${userId}`);
    }

    const newBalance = new Decimal(current.balance).plus(delta).toFixed(8);

    const updated = await tx
      .update(users)
      .set({ balance: newBalance, version: current.version + 1 })
      .where(and(eq(users.id, userId), eq(users.version, current.version)))
      .returning({ id: users.id });

    if (updated.length > 0) {
      return;
    }
  }

  throw new BalanceUpdateConflictError(userId, maxAttempts);
}
