import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Interval } from '@nestjs/schedule';
import { asc, eq, isNull } from 'drizzle-orm';
import { DB, Db } from '../db/db.module';
import { outbox } from '../db/schema';
import { KafkaProducerService } from '../kafka/kafka-producer.service';

@Injectable()
export class OutboxPublisherService {
  private readonly logger = new Logger(OutboxPublisherService.name);
  private readonly topic: string;
  private readonly batchSize: number;
  private isPolling = false;

  constructor(
    @Inject(DB) private readonly db: Db,
    private readonly kafkaProducer: KafkaProducerService,
    private readonly config: ConfigService,
  ) {
    this.topic = this.config.getOrThrow<string>('KAFKA_TOPIC_BALANCE_UPDATES');
    this.batchSize = Number(this.config.get('OUTBOX_BATCH_SIZE') ?? 100);
  }

  @Interval(Number(process.env.OUTBOX_POLL_INTERVAL_MS) || 1000)
  async pollAndPublish(): Promise<void> {
    if (this.isPolling) {
      return;
    }
    this.isPolling = true;
    try {
      const pending = await this.db
        .select()
        .from(outbox)
        .where(isNull(outbox.sentAt))
        .orderBy(asc(outbox.createdAt))
        .limit(this.batchSize);

      for (const record of pending) {
        const payload = record.payload as { user_id: string };
        try {
          await this.kafkaProducer.send(this.topic, payload.user_id, record.payload);
          await this.db.update(outbox).set({ sentAt: new Date() }).where(eq(outbox.id, record.id));
        } catch (err) {
          this.logger.error(`Failed to publish outbox record ${record.id}: ${(err as Error).message}`);
          // Leave sent_at unset — the record will be retried on the next tick.
        }
      }
    } finally {
      this.isPolling = false;
    }
  }
}
