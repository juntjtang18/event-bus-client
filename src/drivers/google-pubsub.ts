import type {
  EventBusDriver,
  EventHandler,
  EventMessage,
  GooglePubSubDriverConfig,
  PublishAck,
  SubscribeOptions,
  SubscriptionHandle
} from '../types';

interface GooglePubSubMessageShape<TPayload = unknown> {
  topic: string;
  payload: TPayload;
  publishedAt: string;
}

type PubSubModule = {
  PubSub: new (options: { projectId: string }) => GooglePubSubClient;
};

type GooglePubSubClient = {
  topic(name: string): GooglePubSubTopic;
  createTopic(name: string): Promise<[GooglePubSubTopic]>;
  subscription(name: string): GooglePubSubSubscription;
  createSubscription(topic: string | GooglePubSubTopic, name: string): Promise<[GooglePubSubSubscription]>;
  close?(): Promise<void>;
};

type GooglePubSubTopic = {
  name: string;
  exists(): Promise<[boolean]>;
  publishMessage(message: {
    data: Buffer;
    attributes?: Record<string, string>;
  }): Promise<string>;
};

type GooglePubSubSubscription = {
  name: string;
  exists(): Promise<[boolean]>;
  on(event: 'message', listener: (message: GoogleSubscriptionMessage) => void): void;
  on(event: 'error', listener: (error: Error) => void): void;
  removeListener(event: 'message', listener: (message: GoogleSubscriptionMessage) => void): void;
  close?(): Promise<void>;
};

type GoogleSubscriptionMessage = {
  id: string;
  data: Buffer;
  attributes: Record<string, string>;
  ack(): void;
  nack(): void;
};

function sanitizeName(value: string): string {
  return value.replace(/[^a-zA-Z0-9-_.~+%]/g, '-');
}

function loadGooglePubSubModule(): PubSubModule {
  try {
    return require('@google-cloud/pubsub') as PubSubModule;
  } catch (error) {
    const reason = error instanceof Error ? ` ${error.message}` : '';
    throw new Error(
      'Driver "google-pubsub" requires the optional peer dependency "@google-cloud/pubsub". Install it in the consuming project before using this driver.' +
        reason
    );
  }
}

export class GooglePubSubDriver implements EventBusDriver {
  private readonly client: GooglePubSubClient;

  constructor(private readonly config: GooglePubSubDriverConfig) {
    const { PubSub } = loadGooglePubSubModule();
    this.client = new PubSub({ projectId: config.projectId });
  }

  async publish<TPayload = unknown>(topic: string, payload: TPayload): Promise<PublishAck> {
    const publishedAt = new Date().toISOString();
    const topicRef = await this.ensureTopic(topic);
    const messageId = await topicRef.publishMessage({
      data: Buffer.from(
        JSON.stringify({
          topic,
          payload,
          publishedAt
        } satisfies GooglePubSubMessageShape<TPayload>)
      ),
      attributes: {
        eventName: topic
      }
    });

    return {
      driver: 'google-pubsub',
      topic,
      messageId,
      publishedAt
    };
  }

  async subscribe<TPayload = unknown>(
    topic: string,
    handler: EventHandler<TPayload>,
    options?: SubscribeOptions
  ): Promise<SubscriptionHandle> {
    const subscription = await this.ensureSubscription(topic, options?.consumerName);

    const listener = async (message: GoogleSubscriptionMessage) => {
      let parsed: GooglePubSubMessageShape<TPayload>;

      try {
        parsed = JSON.parse(message.data.toString('utf8')) as GooglePubSubMessageShape<TPayload>;
      } catch {
        parsed = {
          topic,
          payload: message.data.toString('utf8') as TPayload,
          publishedAt: new Date().toISOString()
        };
      }

      const eventMessage: EventMessage<TPayload> = {
        topic: parsed.topic ?? topic,
        payload: parsed.payload,
        id: message.id,
        attributes: message.attributes,
        publishedAt: parsed.publishedAt,
        raw: message,
        ack: async () => {
          message.ack();
        },
        nack: async () => {
          message.nack();
        }
      };

      await handler(eventMessage);
    };

    const messageListener = (message: GoogleSubscriptionMessage) => {
      void listener(message).catch(() => {
        message.nack();
      });
    };

    subscription.on('message', messageListener);

    subscription.on('error', () => {
      return;
    });

    return {
      driver: 'google-pubsub',
      topic,
      unsubscribe: async () => {
        subscription.removeListener('message', messageListener);
        if (typeof subscription.close === 'function') {
          await subscription.close();
        }
      }
    };
  }

  async close(): Promise<void> {
    if (typeof this.client.close === 'function') {
      await this.client.close();
    }
  }

  private resolveTopicName(topic: string): string {
    return this.config.topicPrefix ? `${this.config.topicPrefix}${topic}` : topic;
  }

  private resolveSubscriptionName(topic: string, consumerName?: string): string {
    const prefix = consumerName ?? this.config.subscriptionPrefix ?? 'event-bus-client';
    return sanitizeName(`${prefix}-${topic}`);
  }

  private async ensureTopic(topic: string): Promise<GooglePubSubTopic> {
    const topicName = this.resolveTopicName(topic);
    const topicRef = this.client.topic(topicName);
    const [exists] = await topicRef.exists();

    if (!exists) {
      if (this.config.autoCreateTopics === false) {
        throw new Error(`Google Pub/Sub topic "${topicName}" does not exist.`);
      }

      const [createdTopic] = await this.client.createTopic(topicName);
      return createdTopic;
    }

    return topicRef;
  }

  private async ensureSubscription(topic: string, consumerName?: string): Promise<GooglePubSubSubscription> {
    const topicRef = await this.ensureTopic(topic);
    const subscriptionName = this.resolveSubscriptionName(topic, consumerName);
    const subscriptionRef = this.client.subscription(subscriptionName);
    const [exists] = await subscriptionRef.exists();

    if (!exists) {
      if (this.config.autoCreateSubscriptions === false) {
        throw new Error(`Google Pub/Sub subscription "${subscriptionName}" does not exist.`);
      }

      const [createdSubscription] = await this.client.createSubscription(topicRef, subscriptionName);
      return createdSubscription;
    }

    return subscriptionRef;
  }
}
