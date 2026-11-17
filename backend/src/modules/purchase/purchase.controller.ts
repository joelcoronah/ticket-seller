import { Body, Controller, Get, HttpCode, MessageEvent, Param, ParseIntPipe, ParseUUIDPipe, Post, Sse } from '@nestjs/common';
import {
  catchError,
  defer,
  filter,
  from,
  interval,
  map,
  merge,
  Observable,
  of,
  share,
  switchMap,
  take,
  takeUntil,
  timer,
} from 'rxjs';
import { UserId } from '../../common/user-id.decorator';
import type { ReserveResult } from '../../domain/types';
import { InventoryService } from '../inventory/inventory.service';
import { RealtimeService } from '../realtime/realtime.service';
import { CreatePurchaseRequestDto } from './purchase.dto';
import { PurchaseService } from './purchase.service';

@Controller('events/:eventId/purchase-requests')
export class PurchaseController {
  constructor(
    private readonly purchase: PurchaseService,
    private readonly inventory: InventoryService,
    private readonly realtime: RealtimeService,
  ) {}

  @Post()
  @HttpCode(202)
  enqueue(
    @Param('eventId', ParseIntPipe) eventId: number,
    @UserId() userId: string,
    @Body() dto: CreatePurchaseRequestDto,
  ) {
    return this.purchase.enqueue(eventId, userId, dto);
  }

  /** Polling fallback (also what k6 uses: k6 core has no SSE client). */
  @Get(':requestId')
  status(@Param('eventId', ParseIntPipe) eventId: number, @Param('requestId', ParseUUIDPipe) requestId: string) {
    return this.purchase.status(eventId, requestId);
  }

  /**
   * SSE stream: `progress` events while waiting, then one `result` event.
   * The client must call `eventSource.close()` on `result` (EventSource auto-reconnects).
   */
  @Sse(':requestId/stream')
  stream(
    @Param('eventId', ParseIntPipe) eventId: number,
    @Param('requestId', ParseUUIDPipe) requestId: string,
  ): Observable<MessageEvent> {
    const resultEvent = (r: ReserveResult): MessageEvent => ({ type: 'result', data: r });

    return defer(() => from(this.purchase.status(eventId, requestId))).pipe(
      switchMap((status) => {
        if (status.state === 'DONE') return of(resultEvent(status.result));

        // Pub/sub is fire-and-forget: a slow re-check covers the race where the
        // result was published between status() and subscribing, or was lost.
        const done$ = merge(
          this.realtime.resultOf(requestId),
          timer(2000, 5000).pipe(
            switchMap(() => this.inventory.getResult(eventId, requestId)),
            filter((r): r is ReserveResult => r !== null),
          ),
        ).pipe(take(1), map(resultEvent), share());

        const progress$ = this.realtime.progressOf(eventId).pipe(
          map((done): MessageEvent => ({
            type: 'progress',
            data: { position: status.position, ahead: Math.max(0, status.position - done - 1) },
          })),
        );
        const ping$ = interval(15_000).pipe(map((): MessageEvent => ({ type: 'ping', data: {} })));

        return merge(
          of<MessageEvent>({ type: 'progress', data: { position: status.position, ahead: status.ahead } }),
          merge(progress$, ping$).pipe(takeUntil(done$)),
          done$,
        );
      }),
      catchError(() => of<MessageEvent>({ type: 'error', data: { code: 'REQUEST_NOT_FOUND' } })),
    );
  }
}
