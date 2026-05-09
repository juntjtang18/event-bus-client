const listenersSymbol = Symbol('listeners');

class MockSubscription {
  constructor(name, topicName) {
    this.name = name;
    this.topicName = topicName;
    this.closed = false;
    this[listenersSymbol] = new Map();
  }

  async exists() {
    return [mockPubSubState.subscriptions.has(this.name)];
  }

  on(event, listener) {
    if (!this[listenersSymbol].has(event)) {
      this[listenersSymbol].set(event, new Set());
    }

    this[listenersSymbol].get(event).add(listener);
  }

  removeListener(event, listener) {
    this[listenersSymbol].get(event)?.delete(listener);
  }

  async close() {
    this.closed = true;
  }

  deliver(message) {
    if (this.closed) {
      return;
    }

    for (const listener of this[listenersSymbol].get('message') ?? []) {
      listener(message);
    }
  }
}

class MockTopic {
  constructor(name) {
    this.name = name;
  }

  async exists() {
    return [mockPubSubState.topics.has(this.name)];
  }

  async publishMessage(message) {
    const id = `msg-${++mockPubSubState.nextMessageId}`;

    for (const subscription of mockPubSubState.subscriptions.values()) {
      if (subscription.topicName !== this.name) {
        continue;
      }

      subscription.deliver({
        id,
        data: message.data,
        attributes: message.attributes ?? {},
        ack() {
          return;
        },
        nack() {
          return;
        }
      });
    }

    return id;
  }
}

const mockPubSubState = {
  nextMessageId: 0,
  topics: new Map(),
  subscriptions: new Map(),
  reset() {
    this.nextMessageId = 0;
    this.topics.clear();
    this.subscriptions.clear();
  }
};

class PubSub {
  constructor(_options) {}

  topic(name) {
    return mockPubSubState.topics.get(name) ?? new MockTopic(name);
  }

  async createTopic(name) {
    const topic = new MockTopic(name);
    mockPubSubState.topics.set(name, topic);
    return [topic];
  }

  subscription(name) {
    return mockPubSubState.subscriptions.get(name) ?? new MockSubscription(name, '');
  }

  async createSubscription(topic, name) {
    const topicName = typeof topic === 'string' ? topic : topic.name;
    const subscription = new MockSubscription(name, topicName);
    mockPubSubState.subscriptions.set(name, subscription);
    return [subscription];
  }

  async close() {
    return;
  }
}

module.exports = {
  PubSub,
  __mockPubSubState: mockPubSubState
};
