// Consumes order + status events, persists them to PostgreSQL and pushes live updates via Socket.io.
const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const { kafka, TOPICS, ensureTopics } = require('./common/kafka');
const db = require('./common/db');
const { canTransition } = require('./common/status');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
app.use(express.static(path.join(__dirname, '..', 'public')));

app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/orders/:id', async (req, res) => {
  const { rows } = await db.query('SELECT * FROM orders WHERE id = $1', [req.params.id]);
  if (!rows[0]) return res.status(404).json({ error: 'not found' });
  const events = await db.query('SELECT status, at FROM order_events WHERE order_id = $1 ORDER BY id', [req.params.id]);
  res.json({ ...rows[0], events: events.rows });
});

io.on('connection', (socket) => {
  socket.on('subscribe', async (orderId) => {
    socket.join(`order:${orderId}`);
    const { rows } = await db.query('SELECT status FROM orders WHERE id = $1', [orderId]);
    if (rows[0]) socket.emit('status', { orderId, status: rows[0].status });   // catch-up on (re)connect
  });
});

async function onOrderCreated(order) {
  // ON CONFLICT makes redelivery of the same Kafka message harmless (idempotent consumer).
  const r = await db.query(
    `INSERT INTO orders (id, customer, items, kitchen_id, status) VALUES ($1,$2,$3,$4,'PLACED')
     ON CONFLICT (id) DO NOTHING`,
    [order.id, order.customer, JSON.stringify(order.items), order.kitchenId]);
  if (r.rowCount) {
    await db.query(`INSERT INTO order_events (order_id, status) VALUES ($1,'PLACED')`, [order.id]);
    io.to(`order:${order.id}`).emit('status', { orderId: order.id, status: 'PLACED' });
  }
}

async function onStatus({ orderId, status }) {
  const { rows } = await db.query('SELECT status FROM orders WHERE id = $1', [orderId]);
  if (!rows[0]) throw new Error('order not persisted yet');           // throw -> kafkajs retries
  if (!canTransition(rows[0].status, status)) return;                 // duplicate or out-of-order
  await db.query('UPDATE orders SET status=$2, updated_at=now() WHERE id=$1', [orderId, status]);
  await db.query('INSERT INTO order_events (order_id, status) VALUES ($1,$2)', [orderId, status]);
  io.to(`order:${orderId}`).emit('status', { orderId, status });
}

(async () => {
  await ensureTopics();
  const consumer = kafka.consumer({ groupId: 'tracking' });
  await consumer.connect();
  await consumer.subscribe({ topics: Object.values(TOPICS), fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const payload = JSON.parse(message.value.toString());
      if (topic === TOPICS.ORDERS) await onOrderCreated(payload);
      else await onStatus(payload);
    },
  });
  server.listen(process.env.PORT || 3002, () => console.log('tracking-service up'));
})().catch((e) => { console.error(e); process.exit(1); });
