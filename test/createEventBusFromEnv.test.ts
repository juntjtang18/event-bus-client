import { afterEach, describe, expect, it } from 'vitest';
import { createEventBus, createEventBusFromEnv } from '../src';

const originalEnv = { ...process.env };

function resetEventBusEnv(): void {
  process.env = { ...originalEnv };
  delete process.env.EVENT_BUS_DRIVER;
  delete process.env.EVENT_BUS_POSTGRES_URL;
  delete process.env.EVENT_BUS_CHANNEL_PREFIX;
}

describe('createEventBusFromEnv', () => {
  afterEach(() => {
    resetEventBusEnv();
  });

  it('uses postgres defaults when driver and channel prefix are not set', () => {
    process.env.EVENT_BUS_POSTGRES_URL = 'postgres://user:password@localhost:5432/event_bus';

    const eventBus = createEventBusFromEnv();

    expect(eventBus.driver).toBe('postgres');
    expect(typeof eventBus.publish).toBe('function');
    expect(typeof eventBus.subscribe).toBe('function');
    expect(typeof eventBus.close).toBe('function');
  });

  it('uses explicit postgres driver and channel prefix from env', () => {
    process.env.EVENT_BUS_DRIVER = 'postgres';
    process.env.EVENT_BUS_POSTGRES_URL = 'postgres://user:password@localhost:5432/event_bus';
    process.env.EVENT_BUS_CHANNEL_PREFIX = 'flashcards';

    const eventBus = createEventBusFromEnv();

    expect(eventBus.driver).toBe('postgres');
    expect(typeof eventBus.publish).toBe('function');
  });

  it('throws a clear error when EVENT_BUS_POSTGRES_URL is missing', () => {
    delete process.env.EVENT_BUS_POSTGRES_URL;

    expect(() => createEventBusFromEnv()).toThrow(
      'Missing required environment variable "EVENT_BUS_POSTGRES_URL" for event-bus-client.'
    );
  });

  it('throws a clear error for unsupported EVENT_BUS_DRIVER values', () => {
    process.env.EVENT_BUS_DRIVER = 'google-pubsub';
    process.env.EVENT_BUS_POSTGRES_URL = 'postgres://user:password@localhost:5432/event_bus';

    expect(() => createEventBusFromEnv()).toThrow(
      'Unsupported EVENT_BUS_DRIVER "google-pubsub". Supported drivers: postgres.'
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
