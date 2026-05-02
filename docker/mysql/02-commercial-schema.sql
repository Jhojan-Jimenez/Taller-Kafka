USE commercial_db;

CREATE TABLE IF NOT EXISTS customers (
  id          VARCHAR(36) PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(100) NOT NULL UNIQUE,
  address     TEXT,
  phone       VARCHAR(20),
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
  id          VARCHAR(36) PRIMARY KEY,
  name        VARCHAR(150) NOT NULL,
  description TEXT,
  price       DECIMAL(10,2) NOT NULL,
  category    VARCHAR(50),
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
  id           VARCHAR(36) PRIMARY KEY,
  customer_id  VARCHAR(36) NOT NULL,
  status       ENUM('PENDING','PAID','CANCELLED','SHIPPED') DEFAULT 'PENDING',
  total_amount DECIMAL(10,2) NOT NULL,
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS order_items (
  id           VARCHAR(36) PRIMARY KEY,
  order_id     VARCHAR(36) NOT NULL,
  product_id   VARCHAR(36) NOT NULL,
  product_name VARCHAR(150) NOT NULL,
  quantity     INT NOT NULL,
  unit_price   DECIMAL(10,2) NOT NULL,
  FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE IF NOT EXISTS payments (
  id         VARCHAR(36) PRIMARY KEY,
  order_id   VARCHAR(36) NOT NULL UNIQUE,
  amount     DECIMAL(10,2) NOT NULL,
  status     ENUM('PENDING','SUCCESS','FAILED') DEFAULT 'PENDING',
  method     VARCHAR(50) DEFAULT 'CREDIT_CARD',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE TABLE IF NOT EXISTS processed_events (
  event_id      VARCHAR(36) PRIMARY KEY,
  event_type    VARCHAR(100) NOT NULL,
  processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
