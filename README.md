# @localitas/pubsub

Browser WebSocket client for Localitas DurablePubSub. Zero dependencies.

## Usage

```html
<script src="localitas-pubsub.min.js"></script>
<script>
  var pubsub = new LocalitasPubSub({
    url: 'ws://localhost:8080/apps/cache/ws/my-cache',
    token: 'your-api-token',
  });

  // Subscribe to a channel (cursor-based, no missed messages)
  pubsub.subscribe('notifications', 'ui-session-123', function(msg) {
    console.log('New:', msg.value);
  });

  // Publish a message
  pubsub.publish('events', '{"type":"click"}', { maxSize: 1000 });

  // Consumer group acknowledgment
  pubsub.ack('jobs', 'workers', msg.seq);

  // Events
  pubsub.on('connected', function() { console.log('connected'); });
  pubsub.on('disconnected', function() { console.log('reconnecting...'); });
</script>
```

## Features

- **Auto-reconnect** with exponential backoff
- **Cursor-based delivery** — no missed messages on reconnect
- **Broadcast** — every subscriber sees every message
- **Consumer groups** — round-robin with acknowledgment
- **Zero dependencies** — pure browser WebSocket API

## Connection

WebSocket connections must land on the Raft leader node. Configure your reverse proxy (FRP) to route `/apps/cache/ws/*` to the leader.

## License

MIT
