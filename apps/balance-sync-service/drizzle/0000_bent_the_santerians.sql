CREATE TABLE IF NOT EXISTS "inbox" (
	"id" varchar(36) PRIMARY KEY NOT NULL,
	"idempotency_id" varchar(255) NOT NULL,
	"user_id" varchar(128) NOT NULL,
	"amount" numeric(20, 8) NOT NULL,
	"processed_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "inbox_idempotency_id_unique" UNIQUE("idempotency_id")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "users" (
	"id" varchar(128) PRIMARY KEY NOT NULL,
	"balance" numeric(20, 8) DEFAULT '0' NOT NULL,
	"version" integer DEFAULT 0 NOT NULL
);
