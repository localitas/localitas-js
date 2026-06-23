/**
 * Localitas DurablePubSub — Browser WebSocket client.
 *
 * Provides real-time pub/sub with automatic reconnection, cursor-based
 * message delivery (no missed messages), and consumer group support.
 *
 * WebSocket connections must land on the Raft leader node. The client
 * auto-reconnects on disconnect, resuming from its last cursor position.
 *
 * Usage:
 *   const pubsub = new LocalitasPubSub({
 *     url: 'ws://localhost:8080/apps/cache/ws/my-cache',
 *     token: 'your-api-token',
 *   });
 *
 *   pubsub.subscribe('notifications', 'ui-session-123', (msg) => {
 *     console.log('New notification:', msg.value);
 *   });
 *
 *   pubsub.publish('events', '{"type":"click"}');
 *
 *   pubsub.on('connected', () => console.log('connected'));
 *   pubsub.on('disconnected', () => console.log('disconnected'));
 */

class LocalitasPubSub {
  /**
   * Create a new DurablePubSub client.
   *
   * @param {Object} opts - Connection options.
   * @param {string} opts.url - WebSocket URL (ws://host/apps/cache/ws/{cache}).
   * @param {string} [opts.token] - Bearer token for authentication.
   * @param {number} [opts.reconnectInterval=2000] - Milliseconds between reconnect attempts.
   * @param {number} [opts.maxReconnectInterval=30000] - Maximum reconnect interval (exponential backoff cap).
   * @param {number} [opts.batchSize=50] - Default number of messages to fetch per read.
   */
  constructor(opts) {
    this.url = opts.url;
    this.token = opts.token || '';
    this.reconnectInterval = opts.reconnectInterval || 2000;
    this.maxReconnectInterval = opts.maxReconnectInterval || 30000;
    this.batchSize = opts.batchSize || 50;

    this._ws = null;
    this._subscriptions = {};  // channel -> { consumer, callback }
    this._listeners = {};      // event -> [callbacks]
    this._reconnectAttempts = 0;
    this._intentionalClose = false;

    this.connect();
  }

  // --- Connection management ---

  /**
   * Connect to the WebSocket server. Called automatically on construction.
   * Handles authentication via query parameter or protocol header.
   */
  connect() {
    var url = this.url;
    if (this.token) {
      url += (url.indexOf('?') >= 0 ? '&' : '?') + 'token=' + encodeURIComponent(this.token);
    }

    this._ws = new WebSocket(url);

    this._ws.onopen = () => {
      this._reconnectAttempts = 0;
      this._emit('connected');

      // Re-subscribe to all channels after reconnect
      for (var channel in this._subscriptions) {
        var sub = this._subscriptions[channel];
        this._send({
          action: 'subscribe',
          channel: channel,
          consumer: sub.consumer,
          count: this.batchSize,
        });
      }
    };

    this._ws.onclose = () => {
      this._emit('disconnected');
      if (!this._intentionalClose) {
        this._reconnect();
      }
    };

    this._ws.onerror = (err) => {
      this._emit('error', err);
    };

    this._ws.onmessage = (event) => {
      var msg;
      try {
        msg = JSON.parse(event.data);
      } catch (e) {
        return;
      }
      this._handleMessage(msg);
    };
  }

  /**
   * Disconnect from the server. Does not auto-reconnect.
   */
  disconnect() {
    this._intentionalClose = true;
    if (this._ws) {
      this._ws.close();
      this._ws = null;
    }
  }

  // --- Pub/Sub operations ---

  /**
   * Subscribe to a channel. The callback is called for each new message.
   * On reconnect, messages are delivered from the last cursor position —
   * no messages are missed.
   *
   * @param {string} channel - Channel name to subscribe to.
   * @param {string} consumer - Consumer ID for cursor tracking.
   * @param {Function} callback - Called with each message: { seq, value, created_at }.
   */
  subscribe(channel, consumer, callback) {
    this._subscriptions[channel] = { consumer: consumer, callback: callback };
    if (this._isConnected()) {
      this._send({
        action: 'subscribe',
        channel: channel,
        consumer: consumer,
        count: this.batchSize,
      });
    }
  }

  /**
   * Unsubscribe from a channel.
   *
   * @param {string} channel - Channel name to unsubscribe from.
   */
  unsubscribe(channel) {
    delete this._subscriptions[channel];
    if (this._isConnected()) {
      this._send({ action: 'unsubscribe', channel: channel });
    }
  }

  /**
   * Publish a message to a channel.
   *
   * @param {string} channel - Channel to publish to.
   * @param {string} value - Message value (typically JSON string).
   * @param {Object} [opts] - Publish options.
   * @param {number} [opts.maxSize] - Bound channel by message count.
   * @param {number} [opts.maxAgeSeconds] - Auto-expire messages older than this.
   */
  publish(channel, value, opts) {
    var msg = { action: 'publish', channel: channel, value: value };
    if (opts) {
      if (opts.maxSize) msg.max_size = opts.maxSize;
      if (opts.maxAgeSeconds) msg.max_age_seconds = opts.maxAgeSeconds;
    }
    this._send(msg);
  }

  /**
   * Acknowledge a consumer group message.
   *
   * @param {string} channel - Channel name.
   * @param {string} group - Consumer group name.
   * @param {number} seq - Sequence number to acknowledge.
   */
  ack(channel, group, seq) {
    this._send({ action: 'ack', channel: channel, group: group, seq: seq });
  }

  // --- Event handling ---

  /**
   * Register an event listener.
   *
   * Events: 'connected', 'disconnected', 'error', 'message', 'published',
   *         'subscribed', 'unsubscribed', 'acked'
   *
   * @param {string} event - Event name.
   * @param {Function} callback - Event handler.
   */
  on(event, callback) {
    if (!this._listeners[event]) {
      this._listeners[event] = [];
    }
    this._listeners[event].push(callback);
  }

  /**
   * Remove an event listener.
   *
   * @param {string} event - Event name.
   * @param {Function} callback - Handler to remove.
   */
  off(event, callback) {
    if (this._listeners[event]) {
      this._listeners[event] = this._listeners[event].filter(function(cb) {
        return cb !== callback;
      });
    }
  }

  // --- Internal ---

  _handleMessage(msg) {
    this._emit(msg.type, msg);

    if (msg.type === 'message' && msg.channel) {
      var sub = this._subscriptions[msg.channel];
      if (sub && sub.callback) {
        sub.callback({
          seq: msg.seq,
          value: msg.value,
          channel: msg.channel,
        });
      }
    }
  }

  _send(data) {
    if (this._isConnected()) {
      this._ws.send(JSON.stringify(data));
    }
  }

  _isConnected() {
    return this._ws && this._ws.readyState === WebSocket.OPEN;
  }

  _reconnect() {
    this._reconnectAttempts++;
    var delay = Math.min(
      this.reconnectInterval * Math.pow(1.5, this._reconnectAttempts - 1),
      this.maxReconnectInterval
    );
    this._emit('reconnecting', { attempt: this._reconnectAttempts, delay: delay });
    setTimeout(() => this.connect(), delay);
  }

  _emit(event, data) {
    if (this._listeners[event]) {
      this._listeners[event].forEach(function(cb) { cb(data); });
    }
  }
}

// Export for both CommonJS and browser global
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { LocalitasPubSub };
} else if (typeof window !== 'undefined') {
  window.LocalitasPubSub = LocalitasPubSub;
}
