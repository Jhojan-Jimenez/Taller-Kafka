const { v4: uuidv4 } = require('uuid');
const kafka = require('../kafka');
const { getPool } = require('../db');

const consumer = kafka.consumer({ groupId: 'billing-group' });
const producer = kafka.producer();

async function startConsumer() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'shipments', fromBeginning: false });

  console.log("[Billing] Consumer started, listening on topic: 'shipments'");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        console.error('[Billing] Invalid message format');
        return;
      }

      if (event.eventType !== 'StockReserved') return;

      console.log(`[Billing] Received from topic '${topic}': ${event.eventType} — orderId: ${event.data.orderId}`);

      const pool = await getPool();

      // Phase 1: idempotency check (auto-committed, outside business transaction)
      const [dup] = await pool.execute(
        'INSERT IGNORE INTO processed_events (event_id, event_type) VALUES (?, ?)',
        [event.eventId, event.eventType]
      );
      if (dup.affectedRows === 0) {
        console.log(`[Billing] Event ${event.eventId} already processed, skipping`);
        return;
      }

      // Phase 2: process payment in transaction
      const { orderId, customerId, customerEmail, customerName, customerAddress, totalAmount, items } = event.data;
      const paymentId = uuidv4();

      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        await conn.execute(
          'INSERT INTO payments (id, order_id, amount, status, method) VALUES (?, ?, ?, ?, ?)',
          [paymentId, orderId, totalAmount, 'SUCCESS', 'CREDIT_CARD']
        );
        await conn.execute("UPDATE orders SET status = 'PAID' WHERE id = ?", [orderId]);
        await conn.commit();
        conn.release();
      } catch (err) {
        await conn.rollback();
        conn.release();
        console.error('[Billing] DB error:', err.message);
        return;
      }

      const paymentEvent = {
        eventId:   uuidv4(),
        eventType: 'PaymentProcessed',
        timestamp: new Date().toISOString(),
        data: {
          paymentId,
          orderId,
          customerId,
          customerEmail,
          customerName,
          customerAddress,
          amount: totalAmount,
          status: 'SUCCESS',
          method: 'CREDIT_CARD',
          items,
        },
      };

      await producer.send({
        topic:    'payments',
        messages: [{ key: orderId, value: JSON.stringify(paymentEvent) }],
      });

      console.log(`[Billing] Published PaymentProcessed → orderId: ${orderId} | amount: $${totalAmount} | status: SUCCESS`);
    },
  });
}

module.exports = { startConsumer };
