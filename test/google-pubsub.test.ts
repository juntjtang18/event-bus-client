import { describe, expect, it } from 'vitest';
import { createEventBus } from '../src';

let sequence = 0;

function uniqueName(prefix: string): string {
  sequence += 1;
  return `${prefix}-${sequence}`;
}

describe('GooglePubSubDriver', () => {
  it('publishes a message and returns a publish acknowledgement', async () => {
    const topic = uniqueName('flashcard.added');
    const eventBus = createEventBus({
      driver: 'google-pubsub',
      config: {
        projectId: 'test-project'
      }
    });

    const ack = await eventBus.publish(topic, {
      userId: 'user-1',
      flashcardId: 'card-1'
    });

    expect(ack.driver).toBe('google-pubsub');
    expect(ack.topic).toBe(topic);
    expect(ack.messageId).toMatch(/^msg-\d+$/);
    expect(ack.publishedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it('delivers a published message to a single subscriber and supports ack', async () => {
    const topic = uniqueName('flashcard.added');
    const eventBus = createEventBus({
      driver: 'google-pubsub',
      config: {
        projectId: 'test-project'
      }
    });

    const received: Array<{
      topic: string;
      payload: unknown;
      id?: string;
      attributes?: Record<string, string>;
    }> = [];

    await eventBus.subscribe(topic, async (message) => {
      received.push({
        topic: message.topic,
        payload: message.payload,
        id: message.id,
        attributes: message.attributes
      });

      await message.ack();
    });

    await eventBus.publish(topic, {
      userId: 'user-1',
      flashcardId: 'card-1'
    });

    expect(received).toHaveLength(1);
    expect(received[0]).toEqual({
      topic,
      payload: {
        userId: 'user-1',
        flashcardId: 'card-1'
      },
      id: expect.stringMatching(/^msg-\d+$/),
      attributes: {
        eventName: topic
      }
    });
  });

  it('delivers one published event to three consumers when each uses its own consumer name', async () => {
    const topic = uniqueName('flashcard.added');
    const eventBus = createEventBus({
      driver: 'google-pubsub',
      config: {
        projectId: 'test-project'
      }
    });

    const consumerPayloads = {
      consumerA: [] as unknown[],
      consumerB: [] as unknown[],
      consumerC: [] as unknown[]
    };

    await eventBus.subscribe(
      topic,
      async (message) => {
        consumerPayloads.consumerA.push(message.payload);
        await message.ack();
      },
      { consumerName: uniqueName('consumer-a') }
    );

    await eventBus.subscribe(
      topic,
      async (message) => {
        consumerPayloads.consumerB.push(message.payload);
        await message.ack();
      },
      { consumerName: uniqueName('consumer-b') }
    );

    await eventBus.subscribe(
      topic,
      async (message) => {
        consumerPayloads.consumerC.push(message.payload);
        await message.ack();
      },
      { consumerName: uniqueName('consumer-c') }
    );

    await eventBus.publish(topic, {
      userId: 'user-99',
      flashcardId: 'card-99'
    });

    expect(consumerPayloads.consumerA).toEqual([
      { userId: 'user-99', flashcardId: 'card-99' }
    ]);
    expect(consumerPayloads.consumerB).toEqual([
      { userId: 'user-99', flashcardId: 'card-99' }
    ]);
    expect(consumerPayloads.consumerC).toEqual([
      { userId: 'user-99', flashcardId: 'card-99' }
    ]);
  });

  it('stops delivery after unsubscribe for that consumer', async () => {
    const topic = uniqueName('flashcard.added');
    const eventBus = createEventBus({
      driver: 'google-pubsub',
      config: {
        projectId: 'test-project'
      }
    });

    const received: unknown[] = [];

    const subscription = await eventBus.subscribe(
      topic,
      async (message) => {
        received.push(message.payload);
        await message.ack();
      },
      { consumerName: uniqueName('consumer-a') }
    );

    await subscription.unsubscribe();
    await eventBus.publish(topic, {
      userId: 'user-2',
      flashcardId: 'card-2'
    });

    expect(received).toEqual([]);
  });
});
