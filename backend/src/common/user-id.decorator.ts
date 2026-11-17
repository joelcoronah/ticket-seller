import { createParamDecorator, ExecutionContext, UnauthorizedException } from '@nestjs/common';

const VALID = /^[A-Za-z0-9_-]{1,64}$/;

/**
 * Demo auth: identity comes from the `x-user-id` header. Replace with a JWT
 * guard in production (see docs/IMPLEMENTATION_GUIDE.md, phase 10).
 */
export const UserId = createParamDecorator((_: unknown, ctx: ExecutionContext): string => {
  const req = ctx.switchToHttp().getRequest<{ headers: Record<string, string | string[] | undefined> }>();
  const id = req.headers['x-user-id'];
  if (typeof id !== 'string' || !VALID.test(id)) throw new UnauthorizedException('x-user-id header required');
  return id;
});
