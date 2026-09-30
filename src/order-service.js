// REST entry point: validates an order, assigns a kitchen and publishes it to Kafka.
const express = require('express');
const crypto = require('crypto');
const { kafka, TOPICS, ensureTopics } = require('./common/kafka');
const { pickKitchen, validateOrder } = require('./common/routing');

const kitchens = (process.env.KITCHEN_IDS || 'kitchen-1,kitchen-2').split(',');
const app = express();
app.use(express.json());
app.use((req, res, next) => {          // demo client runs on another origin
  res.set('Access-Control-Allow-Origin', '*');
  res.set('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

const producer = kafka.producer({ idempotent: true });

app.get('/health', (_req, res) => res.json({ ok: true }));

app.post('/orders', async (req, res) => {
  const errors = validateOrder(req.body);
  if (errors.length) return res.status(400).json({ errors });

  const order = {
    id: crypto.randomUUID(),
    customer: req.body.customer.trim(),
    items: req.body.items,
    createdAt: new Date().toISOString(),
  };
  order.kitchenId = pickKitchen(order.id, kitchens);

  try {
    await producer.send({
      topic: TOPICS.ORDERS,
      messages: [{ key: order.kitchenId, value: JSON.stringify(order) }],
    });
    res.status(202).json({ id: order.id, kitchenId: order.kitchenId, status: 'PLACED' });
  } catch (err) {
    console.error('publish failed', err.message);
    res.status(503).json({ error: 'could not accept order, retry' });
  }
});

(async () => {
  await ensureTopics();
  await producer.connect();
  app.listen(process.env.PORT || 3001, () => console.log('order-service up'));
})().catch((e) => { console.error(e); process.exit(1); });
