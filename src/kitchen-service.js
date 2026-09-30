// One instance per kitchen. Consumes only its own orders and emits status events.
const { kafka, TOPICS, ensureTopics } = require('./common/kafka');

const KITCHEN_ID = process.env.KITCHEN_ID;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const STEPS = [                      // [status, simulated delay before it, ms]
  ['ACCEPTED', 500], ['PREPARING', 1500], ['READY', 4000],
  ['OUT_FOR_DELIVERY', 1500], ['DELIVERED', 5000],
];

(async () => {
  await ensureTopics();
  const producer = kafka.producer({ idempotent: true });
  const consumer = kafka.consumer({ groupId: `kitchen-${KITCHEN_ID}` });
  await Promise.all([producer.connect(), consumer.connect()]);
  await consumer.subscribe({ topic: TOPICS.ORDERS, fromBeginning: false });

  await consumer.run({
    partitionsConsumedConcurrently: 3,
    eachMessage: async ({ message }) => {
      const order = JSON.parse(message.value.toString());
      if (order.kitchenId !== KITCHEN_ID) return;
      console.log(`[${KITCHEN_ID}] received ${order.id}`);
      for (const [status, delay] of STEPS) {
        await sleep(delay);
        await producer.send({
          topic: TOPICS.STATUS,
          messages: [{ key: order.id, value: JSON.stringify({
            orderId: order.id, status, kitchenId: KITCHEN_ID, at: new Date().toISOString(),
          }) }],
        });
      }
    },
  });
})().catch((e) => { console.error(e); process.exit(1); });
