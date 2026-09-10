import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ScheduleModule } from '@nestjs/schedule';
import { LoggerModule } from 'nestjs-pino';
import { AppController } from './app.controller';
import { DbModule } from './db/db.module';
import { KafkaModule } from './kafka/kafka.module';
import { OutboxModule } from './outbox/outbox.module';
import { TransactionsModule } from './transactions/transactions.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    LoggerModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        pinoHttp: {
          level: config.get<string>('LOG_LEVEL') ?? 'info',
          // Loki's level auto-detection only recognizes string level names
          // (e.g. "info"), not pino's default numeric levels (e.g. 30).
          formatters: { level: (label: string) => ({ level: label }) },
        },
      }),
    }),
    ScheduleModule.forRoot(),
    DbModule,
    KafkaModule,
    TransactionsModule,
    OutboxModule,
  ],
  controllers: [AppController],
})
export class AppModule {}
