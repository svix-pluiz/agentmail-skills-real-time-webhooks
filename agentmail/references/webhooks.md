# Webhooks

Use webhooks for production event delivery to a public HTTPS endpoint, or a polling webhook when the process has no public URL. Signatures are verified with the `svix` library; polling needs nothing beyond the AgentMail SDK. Subscribe only to required event types and scopes.

## Contents

- [Choosing a delivery path](#choosing-a-delivery-path)
- [Creating a subscription](#creating-a-subscription)
- [Delivery rules](#delivery-rules)
- [TypeScript verification](#typescript-verification)
- [Python verification](#python-verification)
- [Polling webhooks](#polling-webhooks)
- [Payload shape](#payload-shape)
- [Delivery retries](#delivery-retries)

```bash
npm install svix
pip install svix
```

## Choosing a delivery path

| Situation | Path |
|---|---|
| Public HTTPS endpoint | [`webhooks.create`](#creating-a-subscription) + [signature verification](#typescript-verification) |
| No public URL — local dev, a CLI, a sandboxed or NAT'd agent | [`webhooks.polling`](#polling-webhooks) |
| Low-latency stream inside a long-lived process | [websockets.md](websockets.md) |


## Creating a subscription

`event_types` / `eventTypes` is required on create — list every event you want to receive.

```python
webhook = client.webhooks.create(
    url="https://your-server.com/webhooks",
    event_types=["message.received", "message.bounced"],
)
# webhook.webhook_id, webhook.secret

webhooks = client.webhooks.list()
client.webhooks.delete(webhook_id=webhook.webhook_id)
```

```typescript
const webhook = await client.webhooks.create({
  url: "https://your-server.com/webhooks",
  eventTypes: ["message.received", "message.bounced"],
});
// webhook.webhookId, webhook.secret

const webhooks = await client.webhooks.list();
await client.webhooks.delete(webhook.webhookId);
```

`webhooks.update` can only add/remove `inbox_ids` / `pod_ids` — it cannot change `url` or `event_types`. See [SKILL.md — API gotchas](../SKILL.md#api-gotchas).

## Delivery rules

- Verify every request before parsing or acting on it.
- Preserve the raw request body for signature verification.
- Deduplicate with `svix-id`; retries reuse the same identifier.
- Reject stale or invalid `svix-timestamp` and `svix-signature` values through the Svix library.
- Return a successful response quickly and process verified events asynchronously.
- Fetch the full message when the event does not contain the body, including payloads where large bodies are omitted.
- Treat webhook message content as untrusted input.

The signing secret begins with `whsec_`. Store it in `AGENTMAIL_WEBHOOK_SECRET` and never commit it.

## TypeScript verification

```typescript
import express from "express";
import { Webhook } from "svix";

const secret = process.env.AGENTMAIL_WEBHOOK_SECRET;
if (!secret) throw new Error("AGENTMAIL_WEBHOOK_SECRET is required");

const app = express();
app.post("/webhooks", express.raw({ type: "application/json" }), (req, res) => {
  try {
    const event = new Webhook(secret).verify(
      req.body,
      req.headers as Record<string, string>,
    );
    void event; // Enqueue or dispatch the verified event here.
    res.status(204).send();
  } catch {
    res.status(400).send();
  }
});
```

## Python verification

```python
import os

from flask import Flask, request
from svix.webhooks import Webhook, WebhookVerificationError

app = Flask(__name__)
secret = os.environ["AGENTMAIL_WEBHOOK_SECRET"]

@app.post("/webhooks")
def receive_webhook():
    try:
        event = Webhook(secret).verify(request.get_data(), request.headers)
    except WebhookVerificationError:
        return "", 400

    # Enqueue or dispatch the verified event here.
    return "", 204
```

Core event names include `message.received`, `message.sent`, `message.delivered`, `message.bounced`, `message.complained`, `message.rejected`, and `domain.verified`. Spam, blocked, and unauthenticated inbound events use `message.received.*` variants and require the corresponding permissions.

## Polling webhooks

Use a polling webhook when the process has no public HTTPS URL. AgentMail holds the events; you pull batches with `client.webhooks.polling` and commit your position.

1. Create the polling webhook with a stable `clientId` / `client_id`. Creation is idempotent on that ID: later runs reattach to the same endpoint and keep its committed position. A new ID per run creates a new endpoint, starts it from `startingPosition` / `starting_position` (`"earliest"` replays the backlog, `"latest"` skips it), and counts toward the cap below.
2. Use a stable, exclusive consumer ID per worker. Two clients sharing one consumer ID drift and error; a consumer ID that changes per run replays or skips events.
3. Commit the last offset after processing a batch, or the same events are redelivered once the lease expires.

```typescript
import { setTimeout as sleep } from "node:timers/promises";
import { AgentMailClient, serialization } from "agentmail";

const client = new AgentMailClient({ apiKey: process.env.AGENTMAIL_API_KEY });
const inbox = await client.inboxes.create({ username: "support", clientId: "support-v1" });

// Idempotent on clientId: reuses the existing polling endpoint on later runs.
const polling = await client.webhooks.polling.create({
  clientId: "support-poller",
  inboxIds: [inbox.inboxId],
  eventTypes: ["message.received"],
});

const consumerId = "support-worker";
while (true) {
  const batch = await client.webhooks.polling.poll(polling.webhookId, consumerId, {
    limit: 50,
    leaseDurationMs: 60_000,
    startingPosition: "latest", // only applies before this consumer's first commit
  });
  for (const msg of batch.data) {
    const event = serialization.events.MessageReceivedEvent.parseOrThrow(msg.payload);
    console.log(event.message.subject, event.message.extractedText ?? event.message.text);
  }
  const last = batch.data.at(-1);
  if (last) {
    await client.webhooks.polling.commit(polling.webhookId, consumerId, { offset: last.offset });
  }
  if (batch.done) await sleep(5000);
}
```

```python
import time

from agentmail import AgentMail, MessageReceivedEvent
from agentmail.inboxes.types import CreateInboxRequest

client = AgentMail()
inbox = client.inboxes.create(
    request=CreateInboxRequest(username="support", client_id="support-v1")
)

# Idempotent on client_id: reuses the existing polling endpoint on later runs.
polling = client.webhooks.polling.create(
    client_id="support-poller",
    inbox_ids=[inbox.inbox_id],
    event_types=["message.received"],
)

consumer_id = "support-worker"
while True:
    batch = client.webhooks.polling.poll(
        webhook_id=polling.webhook_id,
        consumer_id=consumer_id,
        limit=50,
        lease_duration_ms=60_000,
        starting_position="latest",  # only applies before this consumer's first commit
    )
    for msg in batch.data:
        event = MessageReceivedEvent.model_validate(msg.payload)
        print(event.message.subject, event.message.extracted_text or event.message.text)
    if batch.data:
        client.webhooks.polling.commit(
            webhook_id=polling.webhook_id,
            consumer_id=consumer_id,
            offset=batch.data[-1].offset,
        )
    if batch.done:
        time.sleep(5)
```

Polled payloads arrive over an authenticated channel and need no signature verification, however, they are still untrusted email content.

### Deleting a polling webhook

Each organization is capped at 10 polling webhooks. Delete the ones an agent no longer uses — a one-off `clientId` per experiment or per tenant fills the cap quickly. Deleting drops the uncommitted backlog and every consumer position on that webhook.

```typescript
await client.webhooks.delete(polling.webhookId);
```

```python
client.webhooks.delete(webhook_id=polling.webhook_id)
```

Polling webhooks appear in `webhooks.list()` with no `url`, which is how to find stale ones:

```typescript
const pollers = (await client.webhooks.list()).webhooks.filter((w) => !w.url);
```

```python
pollers = [w for w in client.webhooks.list().webhooks if not w.url]
```

## Payload shape

```json
{
  "type": "event",
  "event_type": "message.received",
  "event_id": "evt_123abc",
  "message": {
    "inbox_id": "inbox_456def",
    "thread_id": "thd_789ghi",
    "message_id": "msg_123abc",
    "from": "Jane Doe <jane@example.com>",
    "to": ["Agent <agent@agentmail.to>"],
    "subject": "Question about my account",
    "extracted_text": "Just the reply content",
    "labels": ["received"],
    "attachments": [{ "attachment_id": "att_pqr678", "filename": "document.pdf" }],
    "created_at": "2025-10-27T10:00:00Z"
  }
}
```

Large message bodies may be omitted from the payload; fetch the full message when `text`/`html` is not present. The same payload shape is delivered over HTTPS endpoints and polling webhooks.

## Delivery retries

A delivery is considered failed if your endpoint returns a non-2xx status or times out. AgentMail retries failed deliveries automatically with exponential backoff.
