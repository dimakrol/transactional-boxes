import { Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'crypto';
import { eq } from 'drizzle-orm';
import { applyBalanceDelta } from '../common/balance.util';
import { DB, Db } from '../db/db.module';
import { inbox } from '../db/schema';

export interface BalanceUpdateMessage {
  idempotency_id: string;
  user_id: string;
  amount: string;
  transaction_id: string;
}

const UNIQUE_VIOLATION = '23505';

@Injectable()
export class InboxService {
  private readonly logger = new Logger(InboxService.name);
  private readonly maxBalanceUpdateAttempts: number;

  constructor(
    @Inject(DB) private readonly db: Db,
    private readonly config: ConfigService,
  ) {
    this.maxBalanceUpdateAttempts = Number(this.config.get('BALANCE_UPDATE_MAX_ATTEMPTS') ?? 5);
  }

  async processMessage(message: BalanceUpdateMessage): Promise<void> {
    const [existing] = await this.db
      .select()
      .from(inbox)
      .where(eq(inbox.idempotencyId, message.idempotency_id))
      .limit(1);

    if (existing) {
      this.logger.log(`Duplicate idempotency_id=${message.idempotency_id}, skipping`);
      return;
    }

    try {
      await this.db.transaction(async (tx) => {
        await tx.insert(inbox).values({
          id: randomUUID(),
          idempotencyId: message.idempotency_id,
          userId: message.user_id,
          amount: message.amount,
        });

        await applyBalanceDelta(tx, message.user_id, message.amount, this.maxBalanceUpdateAttempts);
      });
      this.logger.log(
        `Balance updated user_id=${message.user_id} amount=${message.amount} transaction_id=${message.transaction_id}`,
      );
    } catch (err: unknown) {
      if (this.isUniqueViolation(err)) {
        this.logger.log(`Duplicate idempotency_id=${message.idempotency_id} (race), skipping`);
        return;
      }
      throw err;
    }
  }

  private isUniqueViolation(err: unknown): boolean {
    return typeof err === 'object' && err !== null && (err as { code?: string }).code === UNIQUE_VIOLATION;
  }
}
