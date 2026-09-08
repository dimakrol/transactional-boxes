import { Body, Controller, HttpCode, HttpStatus, Post, Res } from '@nestjs/common';
import type { Response } from 'express';
import { CreateTransactionDto } from './dto/create-transaction.dto';
import { TransactionsService } from './transactions.service';

@Controller('transactions')
export class TransactionsController {
  constructor(private readonly transactionsService: TransactionsService) {}

  @Post()
  @HttpCode(HttpStatus.OK)
  async create(@Body() dto: CreateTransactionDto, @Res({ passthrough: true }) res: Response) {
    const { transaction, wasCreated } = await this.transactionsService.createTransaction(dto);
    res.status(wasCreated ? HttpStatus.CREATED : HttpStatus.OK);
    return transaction;
  }
}
