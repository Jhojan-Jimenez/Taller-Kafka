const { v4: uuidv4 } = require('uuid');
const kafka = require('../kafka');
const { getPool } = require('../db');

const consumer = kafka.consumer({ groupId: 'shipping-group' });
const producer = kafka.producer();

function generateTrackingNumber() {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  let code = 'TRK-';
  for (let i = 0; i < 8; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return code;
}

async function startConsumer() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'shipments', fromBeginning: false });

  console.log("[Shipping] Consumer started, listening on topic: 'shipments'");

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      let event;
      try {
        event = JSON.parse(message.value.toString());
      } catch {
        console.error('[Shipping] Invalid message format');
        return;
      }

      if (event.eventType !== 'StockReserved') return;

      console.log(`[Shipping] Received from topic '${topic}': ${event.eventType} — orderId: ${event.data.orderId}`);

      const pool = await getPool();

      // Phase 1: idempotency check
      const [dup] = await pool.execute(
        'INSERT IGNORE INTO processed_events (event_id, event_type) VALUES (?, ?)',
        [event.eventId, event.eventType]
      );
      if (dup.affectedRows === 0) {
        console.log(`[Shipping] Event ${event.eventId} already processed, skipping`);
        return;
      }

      // Phase 2: create shipment
      const { orderId, customerId, customerEmail, customerName, customerAddress, items } = event.data;
      const shipmentId      = uuidv4();
      const trackingNumber  = generateTrackingNumber();
      const estimatedDelivery = new Date();
      estimatedDelivery.setDate(estimatedDelivery.getDate() + 7);
      const deliveryDate = estimatedDelivery.toISOString().split('T')[0];

      const conn = await pool.getConnection();
      try {
        await conn.beginTransaction();
        await conn.execute(
          'INSERT INTO shipments (id, order_id, customer_id, tracking_number, status, estimated_delivery) VALUES (?, ?, ?, ?, ?, ?)',
          [shipmentId, orderId, customerId, trackingNumber, 'PREPARING', deliveryDate]
        );
        await conn.commit();
        conn.release();
      } catch (err) {
        await conn.rollback();
        conn.release();
        console.error('[Shipping] DB error:', err.message);
        return;
      }

      const shipmentEvent = {
        eventId:   uuidv4(),
        eventType: 'ShipmentCreated',
        timestamp: new Date().toISOString(),
        data: {
          shipmentId,
          orderId,
          customerId,
          customerEmail,
          customerName,
          customerAddress,
          trackingNumber,
          estimatedDelivery: deliveryDate,
          items,
        },
      };

      await producer.send({
        topic:    'shipments',
        messages: [{ key: orderId, value: JSON.stringify(shipmentEvent) }],
      });

      console.log(`[Shipping] Published ShipmentCreated → orderId: ${orderId} | tracking: ${trackingNumber} | delivery: ${deliveryDate}`);
    },
  });
}

module.exports = { startConsumer };
