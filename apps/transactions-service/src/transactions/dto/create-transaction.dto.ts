import { IsNotEmpty, IsNumberString, IsString } from 'class-validator';

export class CreateTransactionDto {
  @IsString()
  @IsNotEmpty()
  idempotency_id: string;

  @IsString()
  @IsNotEmpty()
  user_id: string;

  @IsNumberString({ no_symbols: false })
  amount: string;
}
