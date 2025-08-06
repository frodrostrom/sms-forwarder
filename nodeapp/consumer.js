const amqp = require('amqplib');
const { MongoClient } = require('mongodb');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:rabbit_pass_2025@rabbitmq';
const MONGODB_URL = process.env.MONGODB_URL || 'mongodb://mongo:27017';
const QUEUE_NAME = 'sms-incoming';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function waitForRabbit() {
  console.log('⏳ Čakám 15 sekúnd pred prvým pokusom o pripojenie na RabbitMQ...');
  await sleep(15000);

  while (true) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      console.log('🟢 Pripojenie na RabbitMQ úspešné');
      return connection;
    } catch (err) {
      console.log('🔁 RabbitMQ ešte nie je dostupný, čakám 1s...');
      await sleep(1000);
    }
  }
}

async function startConsumer() {
  const rabbit = await waitForRabbit();
  const channel = await rabbit.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });

  const mongo = new MongoClient(MONGODB_URL);
  await mongo.connect();
  const db = mongo.db('smsdb');
  const collection = db.collection('sms-incoming');

  console.log('🟢 Consumer je pripojený na RabbitMQ aj MongoDB');

  channel.consume(QUEUE_NAME, async msg => {
    if (msg !== null) {
      try {
        const payload = JSON.parse(msg.content.toString());
        await collection.insertOne(payload);
        console.log('💾 Zápis do MongoDB:', payload);
        channel.ack(msg);
      } catch (err) {
        console.error('❌ Chyba pri zápise do Mongo:', err.message);
        channel.nack(msg, false, false);
      }
    }
  });
}

startConsumer().catch(err => {
  console.error('💥 Consumer zlyhal:', err.message);
  process.exit(1);
});
