-- Base de datos ShopTech — datos semilla para el ejercicio

CREATE TABLE IF NOT EXISTS products (
    id      VARCHAR(50)    PRIMARY KEY,
    name    VARCHAR(200)   NOT NULL,
    stock   INTEGER        NOT NULL DEFAULT 0,
    price   NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id          VARCHAR(50)    PRIMARY KEY,
    product_id  VARCHAR(50)    NOT NULL REFERENCES products(id),
    customer_id VARCHAR(50)    NOT NULL,
    quantity    INTEGER        NOT NULL,
    status      VARCHAR(20)    NOT NULL DEFAULT 'confirmed',
    created_at  TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_product_id ON orders(product_id);

-- Datos semilla
INSERT INTO products (id, name, stock, price) VALUES
    ('prod-001', 'Laptop Pro 15"',        50,  1299.99),
    ('prod-002', 'Wireless Headphones',   200,    89.99),
    ('prod-003', 'Mechanical Keyboard',   150,   129.99),
    ('prod-004', 'USB-C Hub 7-port',      300,    49.99),
    ('prod-005', 'Monitor 4K 27"',         30,   599.99)
ON CONFLICT (id) DO NOTHING;

-- Órdenes históricas para que get_previous_order_count tenga datos
INSERT INTO orders (id, product_id, customer_id, quantity, status) VALUES
    ('ord-hist-001', 'prod-001', 'cust-001', 1, 'confirmed'),
    ('ord-hist-002', 'prod-001', 'cust-002', 2, 'confirmed'),
    ('ord-hist-003', 'prod-002', 'cust-001', 3, 'confirmed'),
    ('ord-hist-004', 'prod-003', 'cust-003', 1, 'confirmed')
ON CONFLICT (id) DO NOTHING;
