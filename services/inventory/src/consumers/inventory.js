const { v4: uuidv4 } = require('uuid');
const kafka = require('../kafka');
const { getPool } = require('../db');

const consumer = kafka.consumer({ groupId: 'inventory-group' });
const producer = kafka.producer();

async function startConsumer() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: false });

  console.log("[Inventory] Consumer started, listening on topic: 'orders'");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        console.error('[Inventory] Invalid message format');
        return;
      }

      if (event.eventType !== 'OrderCreated') return;

      console.log(`[Inventory] Received from topic '${topic}': ${event.eventType} — orderId: ${event.data.orderId}`);

      const pool = await getPool();

      // Phase 1: idempotency check
      const [dup] = await pool.execute(
        'INSERT IGNORE INTO processed_events (event_id, event_type) VALUES (?, ?)',
        [event.eventId, event.eventType]
      );
      if (dup.affectedRows === 0) {
        console.log(`[Inventory] Event ${event.eventId} already processed, skipping`);
        return;
      }

      // Phase 2: reserve stock in transaction
      const { orderId, customerId, customerEmail, customerName, customerAddress, items, totalAmount } = event.data;
      let allReserved = true;

      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();

        for (const item of items) {
          // Atomically reserve only if available stock is sufficient
          const [result] = await conn.execute(
            'UPDATE inventory SET reserved_stock = reserved_stock + ? WHERE product_id = ? AND (total_stock - reserved_stock) >= ?',
            [item.quantity, item.productId, item.quantity]
          );
          if (result.affectedRows === 0) {
            allReserved = false;
            console.log(`[Inventory] Insufficient stock for product: ${item.productId}`);
            break;
          }
        }

        if (allReserved) {
          await conn.commit();
        } else {
          await conn.rollback();
        }
        conn.release();
      } catch (err) {
        await conn.rollback();
        conn.release();
        console.error('[Inventory] DB error:', err.message);
        return;
      }

      if (!allReserved) {
        const failEvent = {
          eventId:   uuidv4(),
          eventType: 'StockInsufficient',
          timestamp: new Date().toISOString(),
          data: { orderId, customerId, customerEmail, customerName, items },
        };
        await producer.send({
          topic:    'shipments',
          messages: [{ key: orderId, value: JSON.stringify(failEvent) }],
        });
        console.log(`[Inventory] Published StockInsufficient → orderId: ${orderId}`);
        return;
      }

      const stockEvent = {
        eventId:   uuidv4(),
        eventType: 'StockReserved',
        timestamp: new Date().toISOString(),
        data: {
          orderId,
          customerId,
          customerEmail,
          customerName,
          customerAddress,
          items,
          totalAmount,
        },
      };
      await producer.send({
        topic:    'shipments',
        messages: [{ key: orderId, value: JSON.stringify(stockEvent) }],
      });

      console.log(`[Inventory] Published StockReserved → orderId: ${orderId} | products: ${items.map(i => i.productId).join(', ')}`);
    },
  });
}

module.exports = { startConsumer };
