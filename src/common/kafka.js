const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: process.env.SERVICE_NAME || 'app',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: { retries: 15, initialRetryTime: 500 },
});

// Orders are keyed by kitchen, status events by order id -> per-key ordering guaranteed.
const TOPICS = { ORDERS: 'orders.created', STATUS: 'orders.status' };

async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: Object.values(TOPICS).map((topic) => ({ topic, numPartitions: 3 })),
  });
  await admin.disconnect();
}

module.exports = { kafka, TOPICS, ensureTopics };
