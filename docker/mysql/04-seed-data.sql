USE commercial_db;

INSERT INTO customers (id, name, email, address, phone) VALUES
('cust-1', 'Daniel Safo',      'danielsafo@unisabana.edu.co',      'Bogotá, Colombia',       '+57 300 1234567'),
('cust-2', 'Daniel Saavedra',  'daniel.saavedra.fon@gmail.com',    'Medellín, Colombia',     '+57 310 7654321'),
('cust-3', 'Maria García',     'maria.garcia@example.com',         'Cali, Colombia',         '+57 320 1111222'),
('cust-4', 'Carlos López',     'carlos.lopez@example.com',         'Barranquilla, Colombia', '+57 315 3334455');

INSERT INTO products (id, name, description, price, category) VALUES
('prod-1', 'Laptop Dell XPS 15',            'Intel Core i7, 16GB RAM, 512GB SSD',         1200.00, 'Electronics'),
('prod-2', 'Mouse Logitech MX Master 3',    'Mouse inalámbrico ergonómico',                  25.00, 'Peripherals'),
('prod-3', 'Teclado Mecánico Keychron K2',  'Teclado TKL con switches Brown',                80.00, 'Peripherals'),
('prod-4', 'Monitor LG 24" 4K UHD',         'Panel IPS, 99% sRGB, USB-C',                  350.00, 'Electronics'),
('prod-5', 'Auriculares Sony WH-1000XM5',   'Noise cancelling, 30h batería',                150.00, 'Audio');

USE logistics_db;

INSERT INTO inventory (product_id, product_name, total_stock, reserved_stock) VALUES
('prod-1', 'Laptop Dell XPS 15',            50,  0),
('prod-2', 'Mouse Logitech MX Master 3',   200,  0),
('prod-3', 'Teclado Mecánico Keychron K2', 100,  0),
('prod-4', 'Monitor LG 24" 4K UHD',         30,  0),
('prod-5', 'Auriculares Sony WH-1000XM5',   75,  0);
