import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './apps/transactions-service/src/db/schema.ts',
  out: './apps/transactions-service/drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env.DATABASE_URL ?? 'postgres://service_a:service_a@localhost:5433/service_a',
  },
});
