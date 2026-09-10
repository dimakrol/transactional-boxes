import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { Logger as PinoLogger } from 'nestjs-pino';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useLogger(app.get(PinoLogger));

  const port = process.env.PORT ?? 3001;
  await app.listen(port);
  Logger.log(`balance-sync-service listening on port ${port}`, 'Bootstrap');
}

bootstrap();
