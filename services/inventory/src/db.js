const mysql = require('mysql2/promise');

let pool = null;

async function getPool() {
  if (pool) return pool;
  pool = mysql.createPool({
    host:             process.env.DB_HOST     || 'localhost',
    port:             parseInt(process.env.DB_PORT) || 3306,
    user:             process.env.DB_USER     || 'root',
    password:         process.env.DB_PASSWORD || 'root123',
    database:         process.env.DB_NAME     || 'logistics_db',
    waitForConnections: true,
    connectionLimit:  10,
  });
  return pool;
}

module.exports = { getPool };
