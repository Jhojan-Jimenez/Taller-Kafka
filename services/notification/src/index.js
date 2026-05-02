const express = require('express');
const { startConsumer } = require('./consumers/notification');

const app = express();
app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'notification' }));

const PORT = process.env.PORT || 3005;
app.listen(PORT, () => console.log(`[Notification] Service listening on port ${PORT}`));

startConsumer().catch(err => {
  console.error('[Notification] Fatal error:', err.message);
  process.exit(1);
});
