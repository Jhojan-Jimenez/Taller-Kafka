const express = require('express');
const { getPool } = require('../db');

const router = express.Router();

router.get('/:id', async (req, res) => {
  const pool = await getPool();
  const [rows] = await pool.execute(
    'SELECT id, name, description, price, category FROM products WHERE id = ?',
    [req.params.id]
  );
  if (rows.length === 0) {
    return res.status(404).json({ error: `Product '${req.params.id}' not found` });
  }
  res.json(rows[0]);
});

module.exports = router;
