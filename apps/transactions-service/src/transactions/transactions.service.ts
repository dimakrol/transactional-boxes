import { ConflictException, Inject, Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'crypto';
import Decimal from 'decimal.js';
import { eq } from 'drizzle-orm';
import { applyBalanceDelta, BalanceUpdateConflictError } from '../common/balance.util';
import { DB, Db } from '../db/db.module';
import { outbox, transactions } from '../db/schema';
import { CreateTransactionDto } from './dto/create-transaction.dto';

export type TransactionRecord = typeof transactions.$inferSelect;

export interface CreateTransactionResult {
  transaction: TransactionRecord;
  wasCreated: boolean;
}

const UNIQUE_VIOLATION = '23505';

@Injectable()
export class TransactionsService {
  private readonly logger = new Logger(TransactionsService.name);
  private readonly maxBalanceUpdateAttempts: number;

  constructor(
    @Inject(DB) private readonly db: Db,
    private readonly config: ConfigService,
  ) {
    this.maxBalanceUpdateAttempts = Number(this.config.get('BALANCE_UPDATE_MAX_ATTEMPTS') ?? 5);
  }

  async createTransaction(dto: CreateTransactionDto): Promise<CreateTransactionResult> {
    const existingBefore = await this.findByIdempotencyId(dto.idempotency_id);
    if (existingBefore) {
      this.logger.log(`Duplicate idempotency_id=${dto.idempotency_id}, returning existing transaction`);
      return { transaction: existingBefore, wasCreated: false };
    }

    const amount = new Decimal(dto.amount).toFixed(8);

    try {
      const created = await this.db.transaction(async (tx) => {
        await applyBalanceDelta(tx, dto.user_id, amount, this.maxBalanceUpdateAttempts).catch((err) => {
          if (err instanceof BalanceUpdateConflictError) {
            throw new ConflictException(err.message);
          }
          throw err;
        });

        const id = randomUUID();
        const [transaction] = await tx
          .insert(transactions)
          .values({ id, idempotencyId: dto.idempotency_id, userId: dto.user_id, amount })
          .returning();

        await tx.insert(outbox).values({
          id: randomUUID(),
          payload: {
            idempotency_id: dto.idempotency_id,
            user_id: dto.user_id,
            amount,
            transaction_id: id,
          },
        });

        return transaction;
      });

      this.logger.log(`Transaction created id=${created.id} user_id=${dto.user_id} amount=${amount}`);
      return { transaction: created, wasCreated: true };
    } catch (err: unknown) {
      if (this.isUniqueViolation(err)) {
        // Lost the race to a concurrent request with the same idempotency_id.
        const existingAfter = await this.findByIdempotencyId(dto.idempotency_id);
        if (existingAfter) {
          return { transaction: existingAfter, wasCreated: false };
        }
      }
      throw err;
    }
  }

  private async findByIdempotencyId(idempotencyId: string): Promise<TransactionRecord | undefined> {
    const [existing] = await this.db
      .select()
      .from(transactions)
      .where(eq(transactions.idempotencyId, idempotencyId))
      .limit(1);
    return existing;
  }

  private isUniqueViolation(err: unknown): boolean {
    return typeof err === 'object' && err !== null && (err as { code?: string }).code === UNIQUE_VIOLATION;
  }
}
