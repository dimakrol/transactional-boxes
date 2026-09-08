import { integer, numeric, pgTable, timestamp, varchar } from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: varchar('id', { length: 128 }).primaryKey(),
  balance: numeric('balance', { precision: 20, scale: 8 }).notNull().default('0'),
  version: integer('version').notNull().default(0),
});

export const inbox = pgTable('inbox', {
  id: varchar('id', { length: 36 }).primaryKey(),
  idempotencyId: varchar('idempotency_id', { length: 255 }).notNull().unique(),
  userId: varchar('user_id', { length: 128 }).notNull(),
  amount: numeric('amount', { precision: 20, scale: 8 }).notNull(),
  processedAt: timestamp('processed_at', { withTimezone: true }).notNull().defaultNow(),
});
