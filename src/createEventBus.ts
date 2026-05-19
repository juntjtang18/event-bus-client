import { resolveLogger } from './logger';
import { GooglePubSubDriver } from './drivers/google-pubsub';
import { PostgresDriver } from './drivers/postgres';
import { RabbitMqDriver } from './drivers/rabbitmq';
import type { CreateEventBusOptions, EventBus, EventBusDriver, EventBusLogger } from './types';

function createDriver(options: CreateEventBusOptions, logger: EventBusLogger): EventBusDriver {
  switch (options.driver) {
    case 'google-pubsub':
      return new GooglePubSubDriver(options.config, logger);
    case 'rabbitmq':
      return new RabbitMqDriver(options.config, logger);
    case 'postgres':
      return new PostgresDriver(options.config, logger);
    default: {
      const exhaustivenessCheck: never = options;
      throw new Error(`Unsupported event bus driver: ${String(exhaustivenessCheck)}`);
    }
  }
}

export function createEventBus(options: CreateEventBusOptions): EventBus {
  const logger = resolveLogger(options);
  logger.info('[event-bus-client] initializing event bus', {
    driver: options.driver
  });

  const driver = createDriver(options, logger);

  return {
    driver: options.driver,
    publish: driver.publish.bind(driver),
    subscribe: driver.subscribe.bind(driver),
    close: async () => {
      logger.info('[event-bus-client] closing event bus', {
        driver: options.driver
      });
      await driver.close();
    }
  };
}

export function createEventBusFromEnv(): EventBus {
  const driver = process.env.EVENT_BUS_DRIVER ?? 'google-pubsub';

  switch (driver) {
    case 'google-pubsub':
      return createEventBus({
        driver,
        config: {
          projectId: getRequiredEnvVar('GCP_PROJECT_ID'),
          topicPrefix: process.env.EVENT_BUS_TOPIC_PREFIX,
          subscriptionPrefix: process.env.EVENT_BUS_SUBSCRIPTION_PREFIX ?? 'event-bus-client'
        }
      });
    case 'postgres':
      return createEventBus({
        driver,
        config: {
          connectionString: getRequiredEnvVar('EVENT_BUS_POSTGRES_URL'),
          channelPrefix: process.env.EVENT_BUS_CHANNEL_PREFIX ?? 'event_bus'
        }
      });
    default:
      throw new Error(
        `Unsupported EVENT_BUS_DRIVER "${driver}". Supported drivers: google-pubsub, postgres.`
      );
  }
}

function getRequiredEnvVar(name: string): string {
  const value = process.env[name];

  if (!value) {
    throw new Error(
      `Missing required environment variable "${name}" for event-bus-client.`
    );
  }

  return value;
}
