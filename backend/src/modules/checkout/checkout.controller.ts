import { Body, Controller, HttpCode, Param, ParseIntPipe, ParseUUIDPipe, Post } from '@nestjs/common';
import { Matches } from 'class-validator';
import { UserId } from '../../common/user-id.decorator';
import { CheckoutService } from './checkout.service';

export class CheckoutDto {
  @Matches(/^[A-Za-z0-9_-]{1,32}$/)
  seatId: string;
}

@Controller('events/:eventId/holds')
export class CheckoutController {
  constructor(private readonly checkout: CheckoutService) {}

  @Post(':holdId/checkout')
  @HttpCode(200)
  pay(
    @Param('eventId', ParseIntPipe) eventId: number,
    @Param('holdId', ParseUUIDPipe) holdId: string,
    @UserId() userId: string,
    @Body() dto: CheckoutDto,
  ) {
    return this.checkout.checkout(eventId, holdId, userId, dto.seatId);
  }
}
