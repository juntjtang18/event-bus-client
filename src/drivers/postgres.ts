import { maskConnectionString } from '../logger';
import type {
  EventBusDriver,
  EventHandler,
  EventBusLogger,
  EventMessage,
  PostgresDriverConfig,
  PublishAck,
  SubscribeOptions,
  SubscriptionHandle
} from '../types';

interface PostgresMessageShape<TPayload = unknown> {
  topic: string;
  payload: TPayload;
  publishedAt: string;
}

type PgModule = {
  Client: new (options: { connectionString: string; keepAlive?: boolean }) => PostgresClient;
};

type PostgresClient = {
  connect(): Promise<void>;
  query(queryText: string, values?: unknown[]): Promise<unknown>;
  on(event: 'notification', listener: (message: PostgresNotification) => void): void;
  on(event: 'error', listener: (error: Error) => void): void;
  on(event: 'end', listener: () => void): void;
  removeListener(event: 'notification', listener: (message: PostgresNotification) => void): void;
  removeAllListeners(): void;
  end(): Promise<void>;
};

type PostgresNotification = {
  channel: string;
  payload?: string;
};

function sanitizeName(value: string): string {
  return value.replace(/[^a-zA-Z0-9_]/g, '_');
}

function loadPgModule(): PgModule {
  try {
    return require('pg') as PgModule;
  } catch (error) {
    const reason = error instanceof Error ? ` ${error.message}` : '';
    throw new Error(
      'Driver "postgres" requires the optional peer dependency "pg". Install it in the consuming project before using this driver.' +
        reason
    );
  }
}

export class PostgresDriver implements EventBusDriver {
  private publisher?: PostgresClient;
  private subscriber?: PostgresClient;
  private subscriberConnectPromise?: Promise<PostgresClient>;
  private reconnectTimer?: NodeJS.Timeout;
  private closing = false;
  private readonly channelHandlers = new Map<string, Set<EventHandler<unknown>>>();
  private readonly notificationListener = (message: PostgresNotification) => {
    void this.handleNotification(message).catch((error) => {
      this.logger.error?.('[event-bus-client] Postgres notification handler failed', {
        driver: 'postgres',
        error
      });
    });
  };

  constructor(
    private readonly config: PostgresDriverConfig,
    private readonly logger: EventBusLogger
  ) {}

  async publish<TPayload = unknown>(topic: string, payload: TPayload): Promise<PublishAck> {
    const client = await this.getPublisher();
    const publishedAt = new Date().toISOString();
    const channel = this.getChannelName(topic);
    this.logger.info('[event-bus-client] publishing event', {
      driver: 'postgres',
      topic,
      channel
    });

    try {
      await client.query('SELECT pg_notify($1, $2)', [
        channel,
        JSON.stringify({
          topic,
          payload,
          publishedAt
        } satisfies PostgresMessageShape<TPayload>)
      ]);
    } catch (error) {
      if (this.publisher === client) {
        this.publisher = undefined;
      }
      this.logger.error?.('[event-bus-client] Postgres publish failed', {
        driver: 'postgres',
        topic,
        error
      });
      throw error;
    }

    return {
      driver: 'postgres',
      topic,
      publishedAt
    };
  }

  async subscribe<TPayload = unknown>(
    topic: string,
    handler: EventHandler<TPayload>,
    _options?: SubscribeOptions
  ): Promise<SubscriptionHandle> {
    const client = await this.getSubscriber();
    const channel = this.getChannelName(topic);
    const handlers = this.channelHandlers.get(channel) ?? new Set<EventHandler<unknown>>();
    const shouldListen = handlers.size === 0;

    this.logger.info('[event-bus-client] subscribing to event', {
      driver: 'postgres',
      topic,
      channel
    });
    if (shouldListen) {
      try {
        await client.query(`LISTEN ${channel}`);
      } catch (error) {
        if (this.subscriber === client) {
          this.subscriber = undefined;
        }
        this.logger.error?.('[event-bus-client] Postgres LISTEN failed', {
          driver: 'postgres',
          topic,
          channel,
          error
        });
        throw error;
      }
    }
    handlers.add(handler as EventHandler<unknown>);
    this.channelHandlers.set(channel, handlers);

    return {
      driver: 'postgres',
      topic,
      unsubscribe: async () => {
        this.logger.info('[event-bus-client] unsubscribing from event', {
          driver: 'postgres',
          topic,
          channel
        });
        const currentHandlers = this.channelHandlers.get(channel);
        if (!currentHandlers) {
          return;
        }
        currentHandlers.delete(handler as EventHandler<unknown>);
        if (currentHandlers.size === 0) {
          this.channelHandlers.delete(channel);
          try {
            await this.subscriber?.query(`UNLISTEN ${channel}`);
          } catch (error) {
            this.logger.error?.('[event-bus-client] Postgres UNLISTEN failed', {
              driver: 'postgres',
              topic,
              channel,
              error
            });
          }
        }
      }
    };
  }

  async close(): Promise<void> {
    this.logger.info('[event-bus-client] closing Postgres driver', {
      driver: 'postgres'
    });
    this.closing = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    if (this.publisher) {
      this.publisher.removeAllListeners();
      await this.publisher.end();
      this.publisher = undefined;
    }

    if (this.subscriber) {
      this.subscriber.removeAllListeners();
      await this.subscriber.end();
      this.subscriber = undefined;
    }
    this.channelHandlers.clear();
  }

  private async getPublisher(): Promise<PostgresClient> {
    if (this.publisher) {
      return this.publisher;
    }

    const { Client } = loadPgModule();
    this.logger.info('[event-bus-client] connecting Postgres publisher', {
      driver: 'postgres',
      connectionString: maskConnectionString(this.config.connectionString)
    });
    this.publisher = new Client({ connectionString: this.config.connectionString, keepAlive: true });
    const publisher = this.publisher;
    publisher.on('error', (error) => {
      this.logger.error?.('[event-bus-client] Postgres publisher connection error', {
        driver: 'postgres',
        error
      });
      if (this.publisher === publisher) {
        this.publisher = undefined;
      }
    });
    publisher.on('end', () => {
      if (this.publisher === publisher) {
        this.publisher = undefined;
      }
    });
    await this.publisher.connect();
    return this.publisher;
  }

  private async getSubscriber(): Promise<PostgresClient> {
    if (this.subscriber) {
      return this.subscriber;
    }
    if (this.subscriberConnectPromise) {
      return this.subscriberConnectPromise;
    }

    this.subscriberConnectPromise = this.connectSubscriber();
    try {
      return await this.subscriberConnectPromise;
    } finally {
      this.subscriberConnectPromise = undefined;
    }
  }

  private async connectSubscriber(): Promise<PostgresClient> {
    const { Client } = loadPgModule();
    this.logger.info('[event-bus-client] connecting Postgres subscriber', {
      driver: 'postgres',
      connectionString: maskConnectionString(this.config.connectionString)
    });
    const subscriber = new Client({ connectionString: this.config.connectionString, keepAlive: true });
    subscriber.on('notification', this.notificationListener);
    subscriber.on('error', (error) => {
      this.logger.error?.('[event-bus-client] Postgres subscriber connection error', {
        driver: 'postgres',
        error
      });
      this.handleSubscriberDisconnect(subscriber);
    });
    subscriber.on('end', () => {
      this.handleSubscriberDisconnect(subscriber);
    });

    await subscriber.connect();
    this.subscriber = subscriber;
    for (const channel of this.channelHandlers.keys()) {
      await subscriber.query(`LISTEN ${channel}`);
    }
    return subscriber;
  }

  private getChannelName(topic: string): string {
    const prefix = this.config.channelPrefix ?? 'event_bus';
    return sanitizeName(`${prefix}_${topic}`);
  }

  private async handleNotification(message: PostgresNotification): Promise<void> {
    if (!message.channel || !message.payload) {
      return;
    }

    const handlers = this.channelHandlers.get(message.channel);
    if (!handlers || handlers.size === 0) {
      return;
    }

    const parsed = JSON.parse(message.payload) as PostgresMessageShape<unknown>;
    const eventMessage: EventMessage<unknown> = {
      topic: parsed.topic ?? '',
      payload: parsed.payload,
      publishedAt: parsed.publishedAt,
      raw: message,
      ack: async () => {
        return;
      },
      nack: async () => {
        return;
      }
    };

    await Promise.all(Array.from(handlers, async (registeredHandler) => {
      await registeredHandler(eventMessage);
    }));
  }

  private handleSubscriberDisconnect(subscriber: PostgresClient): void {
    if (this.closing || this.subscriber !== subscriber) {
      return;
    }
    this.subscriber = undefined;
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.channelHandlers.size === 0) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.reconnectSubscriber();
    }, 1000);
  }

  private async reconnectSubscriber(): Promise<void> {
    let delayMs = 1000;
    while (!this.closing && this.channelHandlers.size > 0 && !this.subscriber) {
      try {
        await this.getSubscriber();
        this.logger.info('[event-bus-client] Postgres subscriber reconnected', {
          driver: 'postgres'
        });
        return;
      } catch (error) {
        this.logger.error?.('[event-bus-client] failed to reconnect Postgres subscriber', {
          driver: 'postgres',
          error
        });
        await new Promise((resolve) => setTimeout(resolve, delayMs));
        delayMs = Math.min(delayMs * 2, 30000);
      }
    }
  }
}
