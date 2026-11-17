import { IsOptional, IsUUID, Matches } from 'class-validator';

export class CreatePurchaseRequestDto {
  /** Client-generated idempotency key: retries of the same click never queue twice. */
  @IsUUID()
  requestId: string;

  /** Specific seat, or omit for "best available". */
  @IsOptional()
  @Matches(/^[A-Za-z0-9_-]{1,32}$/)
  seatId?: string;
}
