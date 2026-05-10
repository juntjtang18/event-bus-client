import type { CreateEventBusOptions, EventBusLogger } from './types';

const defaultLogger: EventBusLogger = {
  info(message, meta) {
    if (meta) {
      console.info(message, meta);
      return;
    }

    console.info(message);
  },
  warn(message, meta) {
    if (meta) {
      console.warn(message, meta);
      return;
    }

    console.warn(message);
  },
  error(message, meta) {
    if (meta) {
      console.error(message, meta);
      return;
    }

    console.error(message);
  }
};

export function resolveLogger(options: CreateEventBusOptions): EventBusLogger {
  return options.logger ?? getDriverLogger(options) ?? defaultLogger;
}

function getDriverLogger(options: CreateEventBusOptions): EventBusLogger | undefined {
  return options.config.logger;
}

export function maskConnectionString(connectionString: string): string {
  try {
    const url = new URL(connectionString);

    if (url.password) {
      url.password = '***';
    }

    if (url.username) {
      url.username = '***';
    }

    return url.toString();
  } catch {
    return '[unparseable connection string]';
  }
}
