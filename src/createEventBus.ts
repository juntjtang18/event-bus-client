import { GooglePubSubDriver } from './drivers/google-pubsub';
import { PostgresDriver } from './drivers/postgres';
import { RabbitMqDriver } from './drivers/rabbitmq';
import type { CreateEventBusOptions, EventBus, EventBusDriver } from './types';

function createDriver(options: CreateEventBusOptions): EventBusDriver {
  switch (options.driver) {
    case 'google-pubsub':
      return new GooglePubSubDriver(options.google);
    case 'rabbitmq':
      return new RabbitMqDriver(options.rabbitmq);
    case 'postgres':
      return new PostgresDriver(options.postgres);
    default: {
      const exhaustivenessCheck: never = options;
      throw new Error(`Unsupported event bus driver: ${String(exhaustivenessCheck)}`);
    }
  }
}

export function createEventBus(options: CreateEventBusOptions): EventBus {
  const driver = createDriver(options);

  return {
    driver: options.driver,
    publish: driver.publish.bind(driver),
    subscribe: driver.subscribe.bind(driver),
    close: driver.close.bind(driver)
  };
}
