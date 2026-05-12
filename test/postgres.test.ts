import { describe, expect, it, vi } from 'vitest';
import { PostgresDriver } from '../src/drivers/postgres';
import type { EventBusLogger } from '../src/types';

class FakePgClient {
  notificationListeners = new Set<(message: { channel: string; payload?: string }) => void>();
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

  on(_event: 'notification', listener: (message: { channel: string; payload?: string }) => void): void {
    this.notificationListeners.add(listener);
  }

  removeListener(_event: 'notification', listener: (message: { channel: string; payload?: string }) => void): void {
    this.notificationListeners.delete(listener);
  }

  async end(): Promise<void> {
    return;
  }

  emitNotification(message: { channel: string; payload?: string }): void {
    for (const listener of this.notificationListeners) {
      listener(message);
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
});
