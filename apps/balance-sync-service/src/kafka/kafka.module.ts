import { Module } from '@nestjs/common';
import { InboxModule } from '../inbox/inbox.module';
import { KafkaConsumerService } from './kafka-consumer.service';

@Module({
  imports: [InboxModule],
  providers: [KafkaConsumerService],
})
export class KafkaModule {}
