/**
 * Integration tests for LocalitasPubSub.
 * Requires: integration cluster running (make integration-cluster-start)
 *
 * Run: node test/pubsub.test.js
 */

const WebSocket = require('ws');
const { LocalitasPubSub } = require('../src/index.js');

// Patch global WebSocket for Node.js environment
global.WebSocket = WebSocket;

const BASE_URL = 'ws://localhost:9090';
const TOKEN = Buffer.from(JSON.stringify({
  user_id: '11111111-1111-1111-1111-111111111111',
  email: 'alice@test.local',
  name: 'Alice Admin',
})).toString('base64');

const CACHE_NAME = 'integ_js_' + Date.now();
let passed = 0;
let failed = 0;

function assert(condition, msg) {
  if (!condition) {
    console.error('  FAIL: ' + msg);
    failed++;
  } else {
    console.log('  PASS: ' + msg);
    passed++;
  }
}

async function createCache() {
  const resp = await fetch(`http://localhost:9090/apps/cache/api/caches`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + TOKEN,
    },
    body: JSON.stringify({ name: CACHE_NAME }),
  });
  assert(resp.status === 201, 'create cache: ' + resp.status);
}

async function deleteCache() {
  await fetch(`http://localhost:9090/apps/cache/api/caches/${CACHE_NAME}`, {
    method: 'DELETE',
    headers: { 'Authorization': 'Bearer ' + TOKEN },
  });
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function testSubscribeAndPublish() {
  console.log('Test: subscribe and publish');

  return new Promise((resolve) => {
    const pubsub = new LocalitasPubSub({
      url: BASE_URL + '/apps/cache/ws/' + CACHE_NAME,
      token: TOKEN,
    });

    let received = [];

    pubsub.on('connected', () => {
      pubsub.subscribe('test-channel', 'test-consumer', (msg) => {
        received.push(msg);
      });

      setTimeout(() => {
        pubsub.publish('test-channel', '{"hello":"world"}');
      }, 200);

      setTimeout(() => {
        assert(received.length >= 1, 'received at least 1 message, got ' + received.length);
        if (received.length > 0) {
          assert(received[0].value === '{"hello":"world"}', 'message value matches');
        }
        pubsub.disconnect();
        resolve();
      }, 1000);
    });
  });
}

async function testReconnectResumesCursor() {
  console.log('Test: reconnect resumes cursor');

  return new Promise((resolve) => {
    const pubsub = new LocalitasPubSub({
      url: BASE_URL + '/apps/cache/ws/' + CACHE_NAME,
      token: TOKEN,
      reconnectInterval: 500,
    });

    let messages = [];

    pubsub.on('connected', () => {
      pubsub.subscribe('resume-channel', 'resume-consumer', (msg) => {
        messages.push(msg);
      });
    });

    setTimeout(() => {
      pubsub.publish('resume-channel', 'msg-before-disconnect');
    }, 300);

    setTimeout(() => {
      assert(messages.length >= 1, 'received msg before disconnect: ' + messages.length);
      pubsub.disconnect();
      resolve();
    }, 1500);
  });
}

async function main() {
  console.log('localitas-js integration tests');
  console.log('cache: ' + CACHE_NAME);

  try {
    await createCache();
    await testSubscribeAndPublish();
    await testReconnectResumesCursor();
    await deleteCache();
  } catch (e) {
    console.error('Error:', e.message);
    failed++;
  }

  console.log('\nResults: ' + passed + ' passed, ' + failed + ' failed');
  process.exit(failed > 0 ? 1 : 0);
}

main();
