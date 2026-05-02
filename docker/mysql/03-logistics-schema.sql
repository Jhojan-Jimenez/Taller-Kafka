USE logistics_db;

CREATE TABLE IF NOT EXISTS inventory (
  product_id      VARCHAR(36) PRIMARY KEY,
  product_name    VARCHAR(150) NOT NULL,
  total_stock     INT NOT NULL DEFAULT 0,
  reserved_stock  INT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS shipments (
  id                 VARCHAR(36) PRIMARY KEY,
  order_id           VARCHAR(36) NOT NULL UNIQUE,
  customer_id        VARCHAR(36) NOT NULL,
  tracking_number    VARCHAR(50) NOT NULL UNIQUE,
  status             ENUM('PREPARING','IN_TRANSIT','DELIVERED') DEFAULT 'PREPARING',
  estimated_delivery DATE,
  created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id      VARCHAR(36) PRIMARY KEY,
  event_type    VARCHAR(100) NOT NULL,
  processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
