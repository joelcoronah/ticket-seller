import 'reflect-metadata';
import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { FastifyAdapter, NestFastifyApplication } from '@nestjs/platform-fastify';
import { ApiModule } from './api.module';
import { appConfig } from './config';

async function bootstrap() {
  // Fastify instead of Express: ~2x requests/s per core, which directly shrinks
  // the time needed to drain a 5k-request burst.
  const adapter = new FastifyAdapter({
    keepAliveTimeout: 65_000, // > typical proxy idle timeout (Traefik)
    bodyLimit: 16 * 1024,
  });
  const app = await NestFactory.create<NestFastifyApplication>(ApiModule, adapter);
  app.enableCors({ origin: appConfig.corsOrigins });
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();

  // A flash sale opens thousands of TCP connections in the same instant. Node's
  // default accept backlog (511) overflows and clients pay a 1-3 s SYN retry.
  // Effective value is capped by net.core.somaxconn (raised in docker-compose).
  await app.listen({ port: appConfig.port, host: '0.0.0.0', backlog: 8192 });
  Logger.log(`API listening on :${appConfig.port}`, 'Bootstrap');
}
void bootstrap();
