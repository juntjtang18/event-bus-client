export type DriverName = 'google-pubsub' | 'postgres' | 'rabbitmq';

export interface EventBusLogger {
  info(message: string, meta?: Record<string, unknown>): void;
  warn?(message: string, meta?: Record<string, unknown>): void;
  error?(message: string, meta?: Record<string, unknown>): void;
}

export interface PublishAck {
  driver: DriverName;
  topic: string;
  messageId?: string;
  publishedAt: string;
}

export interface EventMessage<TPayload = unknown> {
  topic: string;
  payload: TPayload;
  id?: string;
  attributes?: Record<string, string>;
  publishedAt?: string;
  raw?: unknown;
  ack(): Promise<void>;
  nack(): Promise<void>;
}

export type EventHandler<TPayload = unknown> = (
  message: EventMessage<TPayload>
) => Promise<void> | void;

export interface SubscriptionHandle {
  driver: DriverName;
  topic: string;
  unsubscribe(): Promise<void>;
}

export interface SubscribeOptions {
  consumerName?: string;
}

export interface EventBusDriver {
  publish<TPayload = unknown>(topic: string, payload: TPayload): Promise<PublishAck>;
  subscribe<TPayload = unknown>(
    topic: string,
    handler: EventHandler<TPayload>,
    options?: SubscribeOptions
  ): Promise<SubscriptionHandle>;
  close(): Promise<void>;
}

export interface EventBus extends EventBusDriver {
  driver: DriverName;
}

export interface GooglePubSubDriverConfig {
  projectId: string;
  topicPrefix?: string;
  subscriptionPrefix?: string;
  autoCreateTopics?: boolean;
  autoCreateSubscriptions?: boolean;
  logger?: EventBusLogger;
}

export interface RabbitMqDriverConfig {
  url: string;
  exchange?: string;
  exchangeType?: 'topic' | 'direct' | 'fanout' | 'headers';
  queuePrefix?: string;
  durable?: boolean;
  prefetch?: number;
  messagePersistent?: boolean;
  logger?: EventBusLogger;
}

export interface PostgresDriverConfig {
  connectionString: string;
  channelPrefix?: string;
  logger?: EventBusLogger;
}

export interface DriverConfigMap {
  'google-pubsub': GooglePubSubDriverConfig;
  rabbitmq: RabbitMqDriverConfig;
  postgres: PostgresDriverConfig;
}

export type CreateEventBusOptions =
  | {
      driver: 'google-pubsub';
      config: GooglePubSubDriverConfig;
      logger?: EventBusLogger;
    }
  | {
      driver: 'rabbitmq';
      config: RabbitMqDriverConfig;
      logger?: EventBusLogger;
    }
  | {
      driver: 'postgres';
      config: PostgresDriverConfig;
      logger?: EventBusLogger;
    };
