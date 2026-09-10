ALTER TABLE "outbox" ADD COLUMN "attempts" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "outbox" ADD COLUMN "failed_at" timestamp with time zone;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outbox_pending_idx" ON "outbox" USING btree ("created_at") WHERE "outbox"."sent_at" is null and "outbox"."failed_at" is null;