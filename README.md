# @langgo/event-bus-client

Driver-based event bus client for Node.js. It exposes a small `publish` / `subscribe` API and loads the selected transport driver lazily.

## Install

Install the package plus only the SDK for the driver you use.

```bash
npm install @langgo/event-bus-client
```

Google Pub/Sub:

```bash
npm install @google-cloud/pubsub
```

RabbitMQ:

```bash
npm install amqplib
```

Postgres:

```bash
npm install pg
```

## Usage

```ts
import { createEventBus } from '@langgo/event-bus-client';

const eventBus = createEventBus({
  driver: 'google-pubsub',
  google: {
    projectId: process.env.GCP_PROJECT_ID!,
  },
});

await eventBus.publish('flashcard.added', {
  userId: 'u_123',
  flashcardId: 'f_456',
});

await eventBus.subscribe('flashcard.added', async (message) => {
  console.log(message.payload);
  await message.ack();
});
```

## Driver notes

### Google Pub/Sub

- Creates topics automatically by default.
- Creates one subscription per topic using `subscriptionPrefix`.
- Uses JSON payloads internally.

### RabbitMQ

- Uses a topic exchange.
- Creates one queue per topic using `queuePrefix`.
- Manual acknowledgements are enabled.

### Postgres

- Uses `LISTEN` / `NOTIFY`.
- `ack()` and `nack()` are no-ops because Postgres notifications do not support message acknowledgement.
- Payloads are limited by Postgres `NOTIFY` size constraints.

## GitHub Packages

Project publishing is configured for GitHub Packages via `publishConfig.registry`.

Important: GitHub Packages expects the package scope to match the owning GitHub user or organization namespace. If you want to publish `@langgo/event-bus-client`, the package should be published from a GitHub account or organization named `langgo`, or the package scope should be changed to match the actual owner namespace.

Create a local `.npmrc` before publishing:

```ini
@langgo:registry=https://npm.pkg.github.com
//npm.pkg.github.com/:_authToken=${GITHUB_TOKEN}
```

Then publish:

```bash
npm publish
```
