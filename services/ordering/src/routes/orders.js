const express = require('express');
const { v4: uuidv4 } = require('uuid');
const { getPool } = require('../db');
const kafka = require('../kafka');

const router = express.Router();
const producer = kafka.producer();

async function initProducer() {
  await producer.connect();
  console.log('[Ordering] Kafka producer connected');
}

// POST /orders
router.post('/', async (req, res) => {
  const { customerId, items } = req.body;

  if (!customerId || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'customerId and items[] are required' });
  }

  const pool = await getPool();

  try {
    const [customers] = await pool.execute(
      'SELECT id, name, email, address FROM customers WHERE id = ?',
      [customerId]
    );
    if (customers.length === 0) {
      return res.status(404).json({ error: `Customer '${customerId}' not found` });
    }
    const customer = customers[0];

    const productIds = items.map(i => i.productId);
    const placeholders = productIds.map(() => '?').join(',');
    const [products] = await pool.execute(
      `SELECT id, name, price FROM products WHERE id IN (${placeholders})`,
      productIds
    );
    if (products.length !== productIds.length) {
      return res.status(404).json({ error: 'One or more products not found' });
    }

    const productMap = {};
    products.forEach(p => (productMap[p.id] = p));

    let totalAmount = 0;
    const orderItems = items.map(item => {
      const product = productMap[item.productId];
      totalAmount += parseFloat(product.price) * item.quantity;
      return {
        id:          uuidv4(),
        productId:   product.id,
        productName: product.name,
        quantity:    item.quantity,
        unitPrice:   parseFloat(product.price),
      };
    });

    const orderId = uuidv4();

    const conn = await pool.getConnection();
    await conn.beginTransaction();
    try {
      await conn.execute(
        'INSERT INTO orders (id, customer_id, status, total_amount) VALUES (?, ?, ?, ?)',
        [orderId, customerId, 'PENDING', totalAmount]
      );
      for (const item of orderItems) {
        await conn.execute(
          'INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price) VALUES (?, ?, ?, ?, ?, ?)',
          [item.id, orderId, item.productId, item.productName, item.quantity, item.unitPrice]
        );
      }
      await conn.commit();
      conn.release();
    } catch (err) {
      await conn.rollback();
      conn.release();
      throw err;
    }

    const event = {
      eventId:   uuidv4(),
      eventType: 'OrderCreated',
      timestamp: new Date().toISOString(),
      data: {
        orderId,
        customerId:      customer.id,
        customerName:    customer.name,
        customerEmail:   customer.email,
        customerAddress: customer.address,
        items:           orderItems,
        totalAmount,
      },
    };

    await producer.send({
      topic:    'orders',
      messages: [{ key: orderId, value: JSON.stringify(event) }],
    });

    console.log(`[Ordering] Published OrderCreated → orderId: ${orderId} | customer: ${customer.email} | total: $${totalAmount}`);

    res.status(201).json({ orderId, totalAmount, status: 'PENDING', message: 'Order created successfully' });
  } catch (err) {
    console.error('[Ordering] Error:', err.message);
    res.status(500).json({ error: 'Internal server error' });
  }
});

module.exports = { router, initProducer };
