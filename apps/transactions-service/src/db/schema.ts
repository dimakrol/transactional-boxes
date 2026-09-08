import {
  integer,
  jsonb,
  numeric,
  pgTable,
  text,
  timestamp,
  varchar,
} from 'drizzle-orm/pg-core';

export const users = pgTable('users', {
  id: varchar('id', { length: 128 }).primaryKey(),
  balance: numeric('balance', { precision: 20, scale: 8 }).notNull().default('0'),
  version: integer('version').notNull().default(0),
});

export const transactions = pgTable('transactions', {
  id: varchar('id', { length: 36 }).primaryKey(),
  idempotencyId: varchar('idempotency_id', { length: 255 }).notNull().unique(),
  userId: varchar('user_id', { length: 128 }).notNull(),
  amount: numeric('amount', { precision: 20, scale: 8 }).notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
});

export const outbox = pgTable('outbox', {
  id: varchar('id', { length: 36 }).primaryKey(),
  payload: jsonb('payload').notNull(),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  sentAt: timestamp('sent_at', { withTimezone: true }),
});
