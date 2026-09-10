import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Interval } from '@nestjs/schedule';
import { and, asc, eq, isNull } from 'drizzle-orm';
import { DB, Db } from '../db/db.module';
import { outbox } from '../db/schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Injectable()
export class OutboxPublisherService {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private readonly topic: string;
  private readonly batchSize: number;
  private readonly maxAttempts: number;
  private isPolling = false;

  constructor(
    @Inject(DB) private readonly db: Db,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly config: ConfigService,
  ) {
    this.topic = this.config.getOrThrow<string>('KAFKA_TOPIC_BALANCE_UPDATES');
    this.batchSize = Number(this.config.get('OUTBOX_BATCH_SIZE') ?? 100);
    this.maxAttempts = Number(this.config.get('OUTBOX_MAX_ATTEMPTS') ?? 3);
  }

  @Interval(Number(process.env.OUTBOX_POLL_INTERVAL_MS) || 1000)
  async pollAndPublish(): Promise<void> {
    if (this.isPolling) {
      return;
    }
    this.isPolling = true;
    try {
      // Drain the backlog: keep pulling batches back-to-back as long as we're
      // making real progress, instead of waiting for the next timer tick.
      let madeProgress = true;
      while (madeProgress) {
        madeProgress = await this.processBatch();
      }
    } finally {
      this.isPolling = false;
    }
  }

  /**
   * Claims and processes a single batch inside one transaction.
   * Returns true if at least one record was published, signalling that
   * there may be more work waiting immediately.
   */
  private async processBatch(): Promise<boolean> {
    return this.db.transaction(async (tx) => {
      const pending = await tx
        .select()
        .from(outbox)
        .where(and(isNull(outbox.sentAt), isNull(outbox.failedAt)))
        .orderBy(asc(outbox.createdAt))
        .limit(this.batchSize)
        .for('update', { skipLocked: true });

      if (pending.length === 0) {
        return false;
      }

      const results = await Promise.allSettled(
        pending.map(async (record) => {
          const payload = record.payload as { user_id: string };
          await this.kafkaProducer.send(this.topic, payload.user_id, record.payload);
          return record;
        }),
      );

      let progressed = false;
      let publishedCount = 0;
      let failedCount = 0;
      let poisonedCount = 0;
      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        const record = pending[i];
        if (result.status === 'fulfilled') {
          await tx.update(outbox).set({ sentAt: new Date() }).where(eq(outbox.id, record.id));
          progressed = true;
          publishedCount++;
        } else {
          const attempts = record.attempts + 1;
          const failed = attempts >= this.maxAttempts;
          await tx
            .update(outbox)
            .set({ attempts, failedAt: failed ? new Date() : null })
            .where(eq(outbox.id, record.id));
          const reason = (result.reason as Error).message;
          if (failed) {
            poisonedCount++;
            this.logger.error(
              `Outbox record ${record.id} exceeded max attempts (${this.maxAttempts}), giving up: ${reason}`,
            );
          } else {
            failedCount++;
            this.logger.error(
              `Failed to publish outbox record ${record.id} (attempt ${attempts}/${this.maxAttempts}): ${reason}`,
            );
          }
        }
      }

      this.logger.log(
        `Batch processed: ${publishedCount} published, ${failedCount} failed, ${poisonedCount} poisoned`,
      );

      return progressed;
    });
  }
}
