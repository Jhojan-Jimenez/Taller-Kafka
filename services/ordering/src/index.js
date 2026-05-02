const express = require('express');
const { router: ordersRouter, initProducer } = require('./routes/orders');
const productsRouter = require('./routes/products');

const app = express();
app.use(express.json());

app.use('/orders', ordersRouter);
app.use('/products', productsRouter);
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'ordering' }));

const PORT = process.env.PORT || 3001;
app.listen(PORT, async () => {
  console.log(`[Ordering] Service listening on port ${PORT}`);
  try {
    await initProducer();
  } catch (err) {
    console.error('[Ordering] Failed to connect Kafka producer:', err.message);
    process.exit(1);
  }
});
