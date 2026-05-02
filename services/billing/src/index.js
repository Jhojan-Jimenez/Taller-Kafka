const express = require('express');
const { startConsumer } = require('./consumers/billing');

const app = express();
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'billing' }));

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => console.log(`[Billing] Service listening on port ${PORT}`));

startConsumer().catch(err => {
  console.error('[Billing] Fatal error:', err.message);
  process.exit(1);
});
