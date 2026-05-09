import type {
  EventBusDriver,
  EventHandler,
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
  Client: new (options: { connectionString: string }) => PostgresClient;
};

type PostgresClient = {
  connect(): Promise<void>;
  query(queryText: string, values?: unknown[]): Promise<unknown>;
  on(event: 'notification', listener: (message: PostgresNotification) => void): void;
  removeListener(event: 'notification', listener: (message: PostgresNotification) => void): void;
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

  constructor(private readonly config: PostgresDriverConfig) {}

  async publish<TPayload = unknown>(topic: string, payload: TPayload): Promise<PublishAck> {
    const client = await this.getPublisher();
    const publishedAt = new Date().toISOString();
    const channel = this.getChannelName(topic);

    await client.query('SELECT pg_notify($1, $2)', [
      channel,
      JSON.stringify({
        topic,
        payload,
        publishedAt
      } satisfies PostgresMessageShape<TPayload>)
    ]);

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

    await client.query(`LISTEN ${channel}`);

    const listener = async (message: PostgresNotification) => {
      if (message.channel !== channel || !message.payload) {
        return;
      }

      const parsed = JSON.parse(message.payload) as PostgresMessageShape<TPayload>;
      const eventMessage: EventMessage<TPayload> = {
        topic: parsed.topic ?? topic,
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

      await handler(eventMessage);
    };

    const notificationListener = (message: PostgresNotification) => {
      void listener(message);
    };

    client.on('notification', notificationListener);

    return {
      driver: 'postgres',
      topic,
      unsubscribe: async () => {
        client.removeListener('notification', notificationListener);
        await client.query(`UNLISTEN ${channel}`);
      }
    };
  }

  async close(): Promise<void> {
    if (this.publisher) {
      await this.publisher.end();
      this.publisher = undefined;
    }

    if (this.subscriber) {
      await this.subscriber.end();
      this.subscriber = undefined;
    }
  }

  private async getPublisher(): Promise<PostgresClient> {
    if (this.publisher) {
      return this.publisher;
    }

    const { Client } = loadPgModule();
    this.publisher = new Client({ connectionString: this.config.connectionString });
    await this.publisher.connect();
    return this.publisher;
  }

  private async getSubscriber(): Promise<PostgresClient> {
    if (this.subscriber) {
      return this.subscriber;
    }

    const { Client } = loadPgModule();
    this.subscriber = new Client({ connectionString: this.config.connectionString });
    await this.subscriber.connect();
    return this.subscriber;
  }

  private getChannelName(topic: string): string {
    const prefix = this.config.channelPrefix ?? 'event_bus';
    return sanitizeName(`${prefix}_${topic}`);
  }
}
