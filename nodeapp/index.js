const express = require('express');
const amqp = require('amqplib');
const crypto = require('crypto');

const PORT = 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME = 'sms-incoming';

let channel;

// Deduplication memory
const processedMessages = new Map();
const TTL = 5 * 60 * 1000; // 5 minutes

function generateMessageId(payload) {
  return crypto.createHash('sha256')
    .update(`${payload.event_type}-${payload.number}-${payload.from}-${payload.text}-${payload.ts}`)
    .digest('hex');
}

async function connectRabbitWithRetry(retries = 30, delay = 2000) {
  for (let i = 0; i < retries; i++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });
      console.log('✅ RabbitMQ connected');
      return;
    } catch (err) {
      console.warn(`❌ RabbitMQ connection failed (${i + 1}/${retries}), retrying...`);
      await new Promise(res => setTimeout(res, delay));
    }
  }
  console.error('💥 RabbitMQ connection failed after maximum retries');
  process.exit(1);
}

function cleanupProcessedMessages() {
  const cutoff = Date.now() - TTL;
  for (const [key, timestamp] of processedMessages.entries()) {
    if (timestamp < cutoff) {
      processedMessages.delete(key);
    }
  }
}

setInterval(cleanupProcessedMessages, 60 * 1000);

async function start() {
  console.log('⏳ Waiting 5 seconds for RabbitMQ to initialize...');
  await new Promise(res => setTimeout(res, 5000));
  await connectRabbitWithRetry();

  const app = express();

  app.use(express.json());

  app.post('/sms', async (req, res) => {
    const payload = req.body;

    console.log('📨 Incoming payload:', payload);

    const isValid =
      payload &&
      payload.event_type === 'message' &&
      typeof payload.number === 'string' &&
      typeof payload.from === 'string' &&
      typeof payload.text === 'string' &&
      typeof payload.ts === 'string';

    if (!isValid) {
      console.warn('⚠️ Invalid payload – acknowledged to prevent retries:', payload);
      return res.status(200).json({ status: 'ignored-invalid' });
    }

    const msgId = generateMessageId(payload);
    const now = Date.now();

    if (processedMessages.has(msgId)) {
      const age = now - processedMessages.get(msgId);
      if (age < TTL) {
        console.warn('⚠️ Duplicate within TTL – ignored:', msgId);
        return res.status(200).json({ status: 'duplicate' });
      }
    }

    processedMessages.set(msgId, now);

    try {
      channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), {
        persistent: true,
      });
      console.log('📥 Message forwarded to RabbitMQ:', payload);
      return res.status(200).json({ status: 'received' });
    } catch (err) {
      console.error('❌ Failed to send to RabbitMQ:', err.message);
      return res.status(200).json({ status: 'error-sending' });
    }
  });

  app.listen(PORT, () => {
    console.log(`🚀 SMS Webhook listener is running on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('❌ Fatal error:', err.message);
  process.exit(1);
});
