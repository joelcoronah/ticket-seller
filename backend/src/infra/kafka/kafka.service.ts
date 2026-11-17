import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Consumer, Kafka, logLevel, Partitioners, Producer } from 'kafkajs';
import { appConfig } from '../../config';
import { BatchingProducer } from './batching-producer';
import { TOPIC_CONFIGS } from './topics';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(KafkaService.name);
  private readonly kafka = new Kafka({
    clientId: `flashsale-${process.pid}`,
    brokers: appConfig.kafkaBrokers,
    logLevel: logLevel.WARN,
    retry: { retries: 10, initialRetryTime: 300 },
  });
  private readonly rawProducer: Producer = this.kafka.producer({
    idempotent: true, // no duplicates on producer retries
    maxInFlightRequests: 1,
    createPartitioner: Partitioners.DefaultPartitioner, // murmur2(key), Java-compatible
  });
  readonly producer = new BatchingProducer(this.rawProducer, { lingerMs: 5, maxBatch: 1000 });
  private readonly consumers: Consumer[] = [];

  async onModuleInit() {
    await this.ensureTopics();
    await this.rawProducer.connect();
  }

  consumer(groupId: string): Consumer {
    const c = this.kafka.consumer({ groupId, sessionTimeout: 30_000, heartbeatInterval: 3_000 });
    this.consumers.push(c);
    return c;
  }

  private async ensureTopics() {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      const created = await admin.createTopics({ topics: TOPIC_CONFIGS, waitForLeaders: true });
      if (created) this.log.log('Kafka topics created');
    } finally {
      await admin.disconnect();
    }
  }

  async onModuleDestroy() {
    await this.producer.flush();
    await Promise.allSettled(this.consumers.map((c) => c.disconnect()));
    await this.rawProducer.disconnect();
  }
}
