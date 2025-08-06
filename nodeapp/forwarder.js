const { MongoClient } = require('mongodb');
const axios = require('axios');

const MONGO_URL = process.env.MONGO_URL;
const DB_NAME = process.env.MONGO_DB_NAME || 'smsdb';
const COLLECTION_IN = 'sms-incoming';
const COLLECTION_OUT = 'sms-outgoing';
const CLIENT_ENDPOINT = process.env.CLIENT_ENDPOINT;

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function start() {
  const client = new MongoClient(MONGO_URL);
  await client.connect();

  console.log(`🔗 Connected to MongoDB at ${MONGO_URL}`);

  const db = client.db(DB_NAME);
  const incoming = db.collection(COLLECTION_IN);
  const outgoing = db.collection(COLLECTION_OUT);

  const changeStream = incoming.watch([{ $match: { operationType: 'insert' } }]);

  console.log(`🟢 Forwarder listening for inserts in ${DB_NAME}.${COLLECTION_IN}`);

  changeStream.on('change', (change) => {
    const doc = change.fullDocument;

    console.log('📥 New SMS inserted:', {
      from: doc.from,
      number: doc.number,
      text: doc.text,
      ts: doc.ts,
      _id: doc._id
    });

    (async () => {
      const transformed = {
        cli: doc.from,
        ddi: doc.number,
        content: doc.text,
        timestamp: typeof doc.ts === 'string'
          ? Math.floor(new Date(doc.ts).getTime() / 1000)
          : doc.ts,
      };

      console.log('🔄 Transformed payload ready:', transformed);

      const delaySeconds = Math.floor(Math.random() * 26) + 5;
      console.log(`⏱ Waiting ${delaySeconds}s before attempting forward of message ${doc._id}`);
      await delay(delaySeconds * 1000);

      let attempt = 0;
      let success = false;
      let lastError = null;

      while (attempt < MAX_RETRIES && !success) {
        try {
          console.log(`🚀 Attempt ${attempt + 1}/${MAX_RETRIES} for message ${doc._id}`);

          const response = await axios.post(CLIENT_ENDPOINT, transformed, {
            headers: { 'Content-Type': 'application/json' }
          });

          console.log(`📡 Response status for message ${doc._id}: ${response.status}`);

          if (response.status === 200) {
            success = true;
            await outgoing.insertOne({
              messageId: doc._id,
              status: 'success',
              forwardedAt: new Date(),
              payload: transformed,
              attempts: attempt + 1
            });
              console.log(`✅ Message ${doc._id} forwarded successfully after ${attempt + 1} attempts:\n`, JSON.stringify(transformed, null, 2));
          } else {
            lastError = `Unexpected status ${response.status}`;
            throw new Error(lastError);
          }
        } catch (err) {
          lastError = err.message;
          console.error(`⚠️ Attempt ${attempt + 1} for message ${doc._id} failed: ${lastError}`);
          attempt++;
          if (attempt < MAX_RETRIES) await delay(RETRY_DELAY_MS);
        }
      }

      if (!success) {
        await outgoing.insertOne({
          messageId: doc._id,
          status: 'fail',
          failedAt: new Date(),
          payload: transformed,
          error: lastError,
          attempts: attempt
        });
        console.error(`❌ Message ${doc._id} failed to forward after ${attempt} attempts: ${lastError}`);
        console.error(`🔁 Tried payload:`, JSON.stringify(transformed, null, 2));
      }
    })();
  });

  changeStream.on('error', (err) => {
    console.error('💥 Change stream error:', err);
    process.exit(1);
  });
}

start();
