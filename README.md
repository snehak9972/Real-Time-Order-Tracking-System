# Real-Time Order Tracking System

Event-driven order pipeline: **Node.js · Apache Kafka · Socket.io · PostgreSQL · Docker**.

```
Client --POST /orders--> order-service --orders.created (key=kitchenId)--> kitchen-1 / kitchen-2
                                                                              |
Client <--Socket.io-- tracking-service <--orders.status (key=orderId)---------+
                            |
                        PostgreSQL (orders, order_events)
```

## Run
```bash
docker compose up --build
# open http://localhost:3002 and click "Place demo order"
curl -X POST localhost:3001/orders -H 'Content-Type: application/json' \
  -d '{"customer":"Sneha","items":[{"name":"Dosa","qty":2}]}'
curl localhost:3002/orders/<id>     # state + full event history
npm test                            # unit tests (no Docker needed)
```

## Design notes
- **Kitchen routing**: deterministic hash -> kitchen; Kafka key = kitchenId, so each kitchen's orders stay ordered within a partition.
- **Idempotent consumers**: `INSERT ... ON CONFLICT DO NOTHING` plus a state machine (`src/common/status.js`) reject duplicate or out-of-order events.
- **Retries**: if a status event arrives before its order row exists, the handler throws and kafkajs retries.
- **Reconnect catch-up**: a client that subscribes late immediately receives the current status.
- **Scaling**: add a kitchen by adding an ID to `KITCHEN_IDS` and a compose service; scale tracking-service behind a Socket.io Redis adapter.

## Next steps
JWT/RBAC on the order API, dead-letter topic for poison messages, deploy services on AWS ECS Fargate with MSK + RDS.
