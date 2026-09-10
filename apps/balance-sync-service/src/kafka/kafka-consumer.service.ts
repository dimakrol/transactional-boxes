import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Consumer, EachMessagePayload, Kafka, Producer } from 'kafkajs';
import { BalanceUpdateMessage, InboxService } from '../inbox/inbox.service';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly dlqProducer: Producer;
  private readonly topic: string;
  private readonly dlqTopic: string;
  private readonly maxProcessingAttempts: number;

  constructor(
    private readonly config: ConfigService,
    private readonly inboxService: InboxService,
  ) {
    this.kafka = new Kafka({
      clientId: this.config.getOrThrow<string>('KAFKA_CLIENT_ID'),
      brokers: this.config.getOrThrow<string>('KAFKA_BROKERS').split(','),
    });
    this.consumer = this.kafka.consumer({
      groupId: this.config.getOrThrow<string>('KAFKA_CONSUMER_GROUP_ID'),
    });
    this.dlqProducer = this.kafka.producer({ allowAutoTopicCreation: false });
    this.topic = this.config.getOrThrow<string>('KAFKA_TOPIC_BALANCE_UPDATES');
    this.dlqTopic = this.config.getOrThrow<string>('KAFKA_TOPIC_BALANCE_UPDATES_DLQ');
    this.maxProcessingAttempts = Number(this.config.get('INBOX_MAX_PROCESSING_ATTEMPTS') ?? 3);
  }

  async onModuleInit(): Promise<void> {
    await this.ensureTopicsExist();

    await this.dlqProducer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: true });

    await this.consumer.run({
      autoCommit: false,
      eachMessage: (payload) => this.handleMessage(payload),
    });

    this.logger.log('Kafka consumer connected and running');
  }

  private async ensureTopicsExist(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [
          { topic: this.topic, numPartitions: 3 },
          { topic: this.dlqTopic, numPartitions: 3 },
        ],
      });
    } finally {
      await admin.disconnect();
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.consumer.disconnect();
    await this.dlqProducer.disconnect();
  }

  private async handleMessage({ topic, partition, message }: EachMessagePayload): Promise<void> {
    const rawValue = message.value?.toString() ?? '';
    this.logger.debug(`Received message at ${topic}[${partition}]@${message.offset}`);
    let lastError: unknown;

    for (let attempt = 1; attempt <= this.maxProcessingAttempts; attempt++) {
      try {
        const parsed = JSON.parse(rawValue) as BalanceUpdateMessage;
        await this.inboxService.processMessage(parsed);
        await this.commitOffset(topic, partition, message.offset);
        return;
      } catch (err) {
        lastError = err;
        this.logger.warn(
          `Attempt ${attempt}/${this.maxProcessingAttempts} failed for message at ` +
            `${topic}[${partition}]@${message.offset}: ${(err as Error).message}`,
        );
      }
    }

    this.logger.error(
      `Exhausted ${this.maxProcessingAttempts} attempts for message at ` +
        `${topic}[${partition}]@${message.offset}, sending to DLQ`,
    );
    await this.dlqProducer.send({
      topic: this.dlqTopic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: { 'x-original-error': String((lastError as Error)?.message ?? 'unknown') },
        },
      ],
    });
    await this.commitOffset(topic, partition, message.offset);
  }

  private async commitOffset(topic: string, partition: number, offset: string): Promise<void> {
    await this.consumer.commitOffsets([{ topic, partition, offset: (BigInt(offset) + 1n).toString() }]);
  }
}
