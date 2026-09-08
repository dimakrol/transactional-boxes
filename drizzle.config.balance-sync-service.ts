import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './apps/balance-sync-service/src/db/schema.ts',
  out: './apps/balance-sync-service/drizzle',
  dialect: 'postgresql',
  dbCredentials: {
    url: process.env.DATABASE_URL ?? 'postgres://service_b:service_b@localhost:5434/service_b',
  },
});
