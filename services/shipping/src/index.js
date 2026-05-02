const express = require('express');
const { startConsumer } = require('./consumers/shipping');

const app = express();
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'shipping' }));

const PORT = process.env.PORT || 3004;
app.listen(PORT, () => console.log(`[Shipping] Service listening on port ${PORT}`));

startConsumer().catch(err => {
  console.error('[Shipping] Fatal error:', err.message);
  process.exit(1);
});
