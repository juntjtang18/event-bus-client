import { maskConnectionString } from '../logger';
import type {
  EventBusDriver,
  EventHandler,
  EventBusLogger,
  EventMessage,
  PublishAck,
  RabbitMqDriverConfig,
  SubscribeOptions,
  SubscriptionHandle
} from '../types';

interface RabbitMessageShape<TPayload = unknown> {
  topic: string;
  payload: TPayload;
  publishedAt: string;
}

type AmqplibModule = {
  connect(url: string): Promise<RabbitConnection>;
};

type RabbitConnection = {
  createChannel(): Promise<RabbitChannel>;
  close(): Promise<void>;
};

type RabbitChannel = {
  assertExchange(exchange: string, type: string, options: { durable: boolean }): Promise<void>;
  assertQueue(
    queue: string,
    options: { durable: boolean }
  ): Promise<{ queue: string }>;
  bindQueue(queue: string, exchange: string, pattern: string): Promise<void>;
  publish(exchange: string, routingKey: string, content: Buffer, options: { persistent: boolean }): boolean;
  prefetch(count: number): Promise<void>;
  consume(
    queue: string,
    onMessage: (message: RabbitConsumedMessage | null) => void,
    options: { noAck: boolean }
  ): Promise<{ consumerTag: string }>;
  cancel(consumerTag: string): Promise<void>;
  ack(message: RabbitConsumedMessage): void;
  nack(message: RabbitConsumedMessage, allUpTo?: boolean, requeue?: boolean): void;
  close(): Promise<void>;
};

type RabbitConsumedMessage = {
  content: Buffer;
  properties: {
    messageId?: string;
    headers?: Record<string, string>;
  };
};

function sanitizeName(value: string): string {
  return value.replace(/[^a-zA-Z0-9-_.]/g, '.');
}

function loadAmqplibModule(): AmqplibModule {
  try {
    return require('amqplib') as AmqplibModule;
  } catch (error) {
    const reason = error instanceof Error ? ` ${error.message}` : '';
    throw new Error(
      'Driver "rabbitmq" requires the optional peer dependency "amqplib". Install it in the consuming project before using this driver.' +
        reason
    );
  }
}

export class RabbitMqDriver implements EventBusDriver {
  private connection?: RabbitConnection;
  private channel?: RabbitChannel;

  constructor(
    private readonly config: RabbitMqDriverConfig,
    private readonly logger: EventBusLogger
  ) {}

  async publish<TPayload = unknown>(topic: string, payload: TPayload): Promise<PublishAck> {
    const channel = await this.getChannel();
    const publishedAt = new Date().toISOString();
    this.logger.info('[event-bus-client] publishing event', {
      driver: 'rabbitmq',
      topic,
      exchange: this.getExchangeName()
    });

    channel.publish(
      this.getExchangeName(),
      topic,
      Buffer.from(
        JSON.stringify({
          topic,
          payload,
          publishedAt
        } satisfies RabbitMessageShape<TPayload>)
      ),
      {
        persistent: this.config.messagePersistent ?? true
      }
    );

    return {
      driver: 'rabbitmq',
      topic,
      publishedAt
    };
  }

  async subscribe<TPayload = unknown>(
    topic: string,
    handler: EventHandler<TPayload>,
    options?: SubscribeOptions
  ): Promise<SubscriptionHandle> {
    const channel = await this.getChannel();
    const queueName = this.getQueueName(topic, options?.consumerName);
    const { queue } = await channel.assertQueue(queueName, {
      durable: this.config.durable ?? true
    });

    await channel.bindQueue(queue, this.getExchangeName(), topic);
    this.logger.info('[event-bus-client] subscribing to event', {
      driver: 'rabbitmq',
      topic,
      queue,
      consumerName: options?.consumerName,
      exchange: this.getExchangeName()
    });

    const { consumerTag } = await channel.consume(
      queue,
      (message) => {
        if (!message) {
          return;
        }

        void this.handleMessage(topic, message, handler).catch(() => {
          channel.nack(message, false, true);
        });
      },
      { noAck: false }
    );

    return {
      driver: 'rabbitmq',
      topic,
      unsubscribe: async () => {
        this.logger.info('[event-bus-client] unsubscribing from event', {
          driver: 'rabbitmq',
          topic,
          queue,
          consumerTag
        });
        await channel.cancel(consumerTag);
      }
    };
  }

  async close(): Promise<void> {
    this.logger.info('[event-bus-client] closing RabbitMQ driver', {
      driver: 'rabbitmq'
    });
    if (this.channel) {
      await this.channel.close();
      this.channel = undefined;
    }

    if (this.connection) {
      await this.connection.close();
      this.connection = undefined;
    }
  }

  private async getChannel(): Promise<RabbitChannel> {
    if (this.channel) {
      return this.channel;
    }

    const amqplib = loadAmqplibModule();
    this.logger.info('[event-bus-client] connecting RabbitMQ client', {
      driver: 'rabbitmq',
      url: maskConnectionString(this.config.url),
      exchange: this.getExchangeName(),
      exchangeType: this.config.exchangeType ?? 'topic'
    });
    this.connection = await amqplib.connect(this.config.url);
    this.channel = await this.connection.createChannel();
    await this.channel.assertExchange(this.getExchangeName(), this.config.exchangeType ?? 'topic', {
      durable: this.config.durable ?? true
    });

    if (this.config.prefetch) {
      await this.channel.prefetch(this.config.prefetch);
    }

    return this.channel;
  }

  private getExchangeName(): string {
    return this.config.exchange ?? 'event-bus';
  }

  private getQueueName(topic: string, consumerName?: string): string {
    const prefix = consumerName ?? this.config.queuePrefix ?? 'event-bus-client';
    return sanitizeName(`${prefix}.${topic}`);
  }

  private async handleMessage<TPayload>(
    topic: string,
    message: RabbitConsumedMessage,
    handler: EventHandler<TPayload>
  ): Promise<void> {
    const channel = await this.getChannel();
    const parsed = JSON.parse(message.content.toString('utf8')) as RabbitMessageShape<TPayload>;

    const eventMessage: EventMessage<TPayload> = {
      topic: parsed.topic ?? topic,
      payload: parsed.payload,
      id: message.properties.messageId,
      attributes: (message.properties.headers ?? {}) as Record<string, string>,
      publishedAt: parsed.publishedAt,
      raw: message,
      ack: async () => {
        channel.ack(message);
      },
      nack: async () => {
        channel.nack(message, false, true);
      }
    };

    await handler(eventMessage);
  }
}
