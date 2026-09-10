import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { LoggerModule } from 'nestjs-pino';
import { AppController } from './app.controller';
import { DbModule } from './db/db.module';
import { InboxModule } from './inbox/inbox.module';
import { KafkaModule } from './kafka/kafka.module';

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
    DbModule,
    InboxModule,
    KafkaModule,
  ],
  controllers: [AppController],
})
export class AppModule {}
