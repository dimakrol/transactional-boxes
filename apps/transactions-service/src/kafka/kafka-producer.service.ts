import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaProducerService.name);
  private readonly kafka: Kafka;
  private readonly producer: Producer;

  private readonly topic: string;

  constructor(private readonly config: ConfigService) {
    this.kafka = new Kafka({
      clientId: this.config.getOrThrow<string>('KAFKA_CLIENT_ID'),
      brokers: this.config.getOrThrow<string>('KAFKA_BROKERS').split(','),
    });
    this.producer = this.kafka.producer({ allowAutoTopicCreation: false });
    this.topic = this.config.getOrThrow<string>('KAFKA_TOPIC_BALANCE_UPDATES');
  }

  async onModuleInit() {
    await this.ensureTopicExists();
    await this.producer.connect();
    this.logger.log('Kafka producer connected');
  }

  private async ensureTopicExists(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic: this.topic, numPartitions: 3 }],
      });
    } finally {
      await admin.disconnect();
    }
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async send(topic: string, key: string, value: unknown): Promise<void> {
    await this.producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(value) }],
    });
  }
}
