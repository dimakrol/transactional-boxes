import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AppController } from './app.controller';
import { DbModule } from './db/db.module';
import { InboxModule } from './inbox/inbox.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), DbModule, InboxModule, KafkaModule],
  controllers: [AppController],
})
export class AppModule {}
