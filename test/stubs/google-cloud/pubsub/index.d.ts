export declare class PubSub {
  constructor(options: { projectId: string });
  topic(name: string): {
    name: string;
    exists(): Promise<[boolean]>;
    publishMessage(message: {
      data: Buffer;
      attributes?: Record<string, string>;
    }): Promise<string>;
  };
  createTopic(name: string): Promise<[unknown]>;
  subscription(name: string): {
    name: string;
    exists(): Promise<[boolean]>;
    on(event: 'message' | 'error', listener: (message: any) => void): void;
    removeListener(event: 'message', listener: (message: any) => void): void;
    close(): Promise<void>;
  };
  createSubscription(topic: string | { name: string }, name: string): Promise<[unknown]>;
  close(): Promise<void>;
}

export declare const __mockPubSubState: {
  nextMessageId: number;
  topics: Map<string, unknown>;
  subscriptions: Map<string, unknown>;
  reset(): void;
};
