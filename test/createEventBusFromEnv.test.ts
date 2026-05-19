import { afterEach, describe, expect, it } from 'vitest';
import { createEventBus, createEventBusFromEnv } from '../src';

const originalEnv = { ...process.env };

function resetEventBusEnv(): void {
  process.env = { ...originalEnv };
  delete process.env.EVENT_BUS_DRIVER;
  delete process.env.GCP_PROJECT_ID;
  delete process.env.EVENT_BUS_TOPIC_PREFIX;
  delete process.env.EVENT_BUS_SUBSCRIPTION_PREFIX;
  delete process.env.EVENT_BUS_POSTGRES_URL;
  delete process.env.EVENT_BUS_CHANNEL_PREFIX;
}

describe('createEventBusFromEnv', () => {
  afterEach(() => {
    resetEventBusEnv();
  });

  it('uses google pubsub defaults when driver and subscription prefix are not set', () => {
    process.env.GCP_PROJECT_ID = 'test-project';

    const eventBus = createEventBusFromEnv();

    expect(eventBus.driver).toBe('google-pubsub');
    expect(typeof eventBus.publish).toBe('function');
    expect(typeof eventBus.subscribe).toBe('function');
    expect(typeof eventBus.close).toBe('function');
  });

  it('uses explicit google pubsub driver and prefixes from env', () => {
    process.env.EVENT_BUS_DRIVER = 'google-pubsub';
    process.env.GCP_PROJECT_ID = 'test-project';
    process.env.EVENT_BUS_TOPIC_PREFIX = 'dev-';
    process.env.EVENT_BUS_SUBSCRIPTION_PREFIX = 'flashcards';

    const eventBus = createEventBusFromEnv();

    expect(eventBus.driver).toBe('google-pubsub');
    expect(typeof eventBus.publish).toBe('function');
  });

  it('uses explicit postgres driver and channel prefix from env', () => {
    process.env.EVENT_BUS_DRIVER = 'postgres';
    process.env.EVENT_BUS_POSTGRES_URL = 'postgres://user:password@localhost:5432/event_bus';
    process.env.EVENT_BUS_CHANNEL_PREFIX = 'flashcards';

    const eventBus = createEventBusFromEnv();

    expect(eventBus.driver).toBe('postgres');
    expect(typeof eventBus.publish).toBe('function');
  });

  it('throws a clear error when GCP_PROJECT_ID is missing for the default driver', () => {
    delete process.env.GCP_PROJECT_ID;

    expect(() => createEventBusFromEnv()).toThrow(
      'Missing required environment variable "GCP_PROJECT_ID" for event-bus-client.'
    );
  });

  it('throws a clear error when EVENT_BUS_POSTGRES_URL is missing', () => {
    process.env.EVENT_BUS_DRIVER = 'postgres';
    delete process.env.EVENT_BUS_POSTGRES_URL;

    expect(() => createEventBusFromEnv()).toThrow(
      'Missing required environment variable "EVENT_BUS_POSTGRES_URL" for event-bus-client.'
    );
  });

  it('throws a clear error for unsupported EVENT_BUS_DRIVER values', () => {
    process.env.EVENT_BUS_DRIVER = 'rabbitmq';
    process.env.GCP_PROJECT_ID = 'test-project';

    expect(() => createEventBusFromEnv()).toThrow(
      'Unsupported EVENT_BUS_DRIVER "rabbitmq". Supported drivers: google-pubsub, postgres.'
    );
  });
});

describe('createEventBus', () => {
  it('accepts the new explicit postgres config shape', () => {
    const eventBus = createEventBus({
      driver: 'postgres',
      config: {
        connectionString: 'postgres://user:password@localhost:5432/event_bus',
        channelPrefix: 'event_bus'
      }
    });

    expect(eventBus.driver).toBe('postgres');
  });
});
