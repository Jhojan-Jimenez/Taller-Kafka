const express = require('express');
const { startConsumer } = require('./consumers/inventory');

const app = express();
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'inventory' }));

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => console.log(`[Inventory] Service listening on port ${PORT}`));

startConsumer().catch(err => {
  console.error('[Inventory] Fatal error:', err.message);
  process.exit(1);
});
