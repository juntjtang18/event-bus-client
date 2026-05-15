import { describe, expect, it, vi } from 'vitest';
import { PostgresDriver } from '../src/drivers/postgres';
import type { EventBusLogger } from '../src/types';

class FakePgClient {
  notificationListeners = new Set<(message: { channel: string; payload?: string }) => void>();
  errorListeners = new Set<(error: Error) => void>();
  endListeners = new Set<() => void>();
  listenCalls: string[] = [];
  unlistenCalls: string[] = [];

  async connect(): Promise<void> {
    return;
  }

  async query(queryText: string): Promise<unknown> {
    if (queryText.startsWith('LISTEN ')) {
      this.listenCalls.push(queryText);
    }
    if (queryText.startsWith('UNLISTEN ')) {
      this.unlistenCalls.push(queryText);
    }
    return {};
  }

  on(event: 'notification' | 'error' | 'end', listener: any): void {
    if (event === 'notification') {
      this.notificationListeners.add(listener);
    }
    if (event === 'error') {
      this.errorListeners.add(listener);
    }
    if (event === 'end') {
      this.endListeners.add(listener);
    }
  }

  removeListener(_event: 'notification', listener: (message: { channel: string; payload?: string }) => void): void {
    this.notificationListeners.delete(listener);
  }

  removeAllListeners(): void {
    this.notificationListeners.clear();
    this.errorListeners.clear();
    this.endListeners.clear();
  }

  async end(): Promise<void> {
    return;
  }

  emitNotification(message: { channel: string; payload?: string }): void {
    for (const listener of this.notificationListeners) {
      listener(message);
    }
  }

  emitError(error: Error): void {
    for (const listener of this.errorListeners) {
      listener(error);
    }
  }
}

const logger: EventBusLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
};

describe('PostgresDriver', () => {
  it('uses one notification listener and dispatches by channel', async () => {
    const driver = new PostgresDriver(
      { connectionString: 'postgres://user:pass@localhost:5432/test', channelPrefix: 'evt' },
      logger
    );

    const fakeClient = new FakePgClient();

    (driver as any).getSubscriber = async function () {
      if (this.subscriber) {
        return this.subscriber;
      }
      this.subscriber = fakeClient;
      this.subscriber.on('notification', this.notificationListener);
      return this.subscriber;
    };

    const handlerA = vi.fn(async () => undefined);
    const handlerB = vi.fn(async () => undefined);

    const subA = await driver.subscribe('flashcard.created', handlerA);
    const subB = await driver.subscribe('flashcard.reviewed', handlerB);

    expect(fakeClient.notificationListeners.size).toBe(1);
    expect(fakeClient.listenCalls).toEqual([
      'LISTEN evt_flashcard_created',
      'LISTEN evt_flashcard_reviewed',
    ]);

    fakeClient.emitNotification({
      channel: 'evt_flashcard_created',
      payload: JSON.stringify({
        topic: 'flashcard.created',
        payload: { eventId: 'flashcard.created:1' },
        publishedAt: '2026-05-12T00:00:00.000Z',
      }),
    });

    await Promise.resolve();

    expect(handlerA).toHaveBeenCalledTimes(1);
    expect(handlerB).toHaveBeenCalledTimes(0);

    await subA.unsubscribe();
    await subB.unsubscribe();

    expect(fakeClient.unlistenCalls).toEqual([
      'UNLISTEN evt_flashcard_created',
      'UNLISTEN evt_flashcard_reviewed',
    ]);
  });

  it('attaches an error listener to the Postgres subscriber client', async () => {
    const driver = new PostgresDriver(
      { connectionString: 'postgres://user:pass@localhost:5432/test', channelPrefix: 'evt' },
      logger
    );
    const fakeClient = new FakePgClient();

    (driver as any).connectSubscriber = async function () {
      fakeClient.on('notification', this.notificationListener);
      fakeClient.on('error', (error: Error) => {
        this.logger.error?.('[event-bus-client] Postgres subscriber connection error', {
          driver: 'postgres',
          error
        });
        this.handleSubscriberDisconnect(fakeClient);
      });
      fakeClient.on('end', () => {
        this.handleSubscriberDisconnect(fakeClient);
      });
      this.subscriber = fakeClient;
      return fakeClient;
    };

    await driver.subscribe('flashcard.created', vi.fn());

    expect(fakeClient.errorListeners.size).toBe(1);
    expect(() => fakeClient.emitError(new Error('Connection terminated unexpectedly'))).not.toThrow();
    expect(logger.error).toHaveBeenCalledWith(
      '[event-bus-client] Postgres subscriber connection error',
      expect.objectContaining({ driver: 'postgres' })
    );
  });
});
