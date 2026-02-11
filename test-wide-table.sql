-- Test wide table for column paging
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  first_name  VARCHAR(50)  NOT NULL,
  last_name   VARCHAR(50)  NOT NULL,
  email       VARCHAR(100) NOT NULL,
  phone       VARCHAR(20),
  address     VARCHAR(200),
  city        VARCHAR(50),
  state       VARCHAR(50),
  zip_code    VARCHAR(10),
  country     VARCHAR(50)  DEFAULT 'China',
  birth_date  DATE,
  gender      ENUM('M','F','O'),
  credit_limit DECIMAL(10,2) DEFAULT 0,
  notes       TEXT,
  metadata    JSON,
  avatar      BLOB,
  created_at  DATETIME     DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE products (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  sku           VARCHAR(30)  NOT NULL UNIQUE,
  name          VARCHAR(120) NOT NULL,
  category      VARCHAR(50),
  sub_category  VARCHAR(50),
  brand         VARCHAR(50),
  description   TEXT,
  specs         JSON,
  price         DECIMAL(10,2) NOT NULL,
  cost          DECIMAL(10,2),
  stock_qty     INT DEFAULT 0,
  weight_kg     DECIMAL(6,2),
  is_active     TINYINT(1) DEFAULT 1,
  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  customer_id   INT NOT NULL,
  order_date    DATETIME DEFAULT CURRENT_TIMESTAMP,
  status        ENUM('pending','processing','shipped','delivered','cancelled') DEFAULT 'pending',
  shipping_addr VARCHAR(200),
  shipping_city VARCHAR(50),
  shipping_zip  VARCHAR(10),
  payment_method VARCHAR(30),
  subtotal      DECIMAL(10,2),
  tax           DECIMAL(10,2),
  shipping_fee  DECIMAL(10,2),
  discount      DECIMAL(10,2) DEFAULT 0,
  total         DECIMAL(10,2),
  tracking_no   VARCHAR(50),
  notes         TEXT,
  created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE order_items (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  order_id    INT NOT NULL,
  product_id  INT NOT NULL,
  quantity    INT NOT NULL DEFAULT 1,
  unit_price  DECIMAL(10,2) NOT NULL,
  line_total  DECIMAL(10,2),
  FOREIGN KEY (order_id)   REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Customers
INSERT INTO customers (first_name, last_name, email, phone, address, city, state, zip_code, country, birth_date, gender, credit_limit, notes, metadata) VALUES
('Wei',    'Zhang',  'wei.zhang@example.com',    '13800001111', '88 Nanjing Road',       'Shanghai',  'Shanghai',  '200001', 'China', '1990-03-15', 'M', 50000.00, 'VIP customer since 2020, prefers express shipping',           '{"tier":"gold","points":12800,"preferences":{"lang":"zh","notifications":true}}'),
('Mei',    'Li',     'mei.li@example.com',       '13900002222', '66 Wangfujing Street',  'Beijing',   'Beijing',   '100006', 'China', '1985-07-22', 'F', 30000.00, NULL,                                                           '{"tier":"silver","points":5400}'),
('Jun',    'Wang',   'jun.wang@example.com',      '13700003333', '12 Tianhe Road',        'Guangzhou', 'Guangdong', '510620', 'China', '1992-11-08', 'M', 20000.00, 'Frequent buyer, interested in electronics and books',         '{"tier":"silver","points":7200,"preferences":{"lang":"zh","notifications":false}}'),
('Xia',    'Chen',   'xia.chen@example.com',      '13600004444', '99 Jiefang Road',       'Wuhan',     'Hubei',     '430000', 'China', '1988-01-30', 'F', 80000.00, 'Corporate account manager\nHandles bulk orders for tech dept', '{"tier":"platinum","points":34500,"company":"TechCorp"}'),
('Hao',    'Liu',    'hao.liu@example.com',        '13500005555', '45 Chunxi Road',        'Chengdu',   'Sichuan',   '610000', 'China', '1995-06-12', 'M', 15000.00, NULL,                                                           '{"tier":"bronze","points":1200}'),
('Yan',    'Yang',   'yan.yang@example.com',       '13400006666', '7 Zhongshan Road',      'Nanjing',   'Jiangsu',   '210000', 'China', '1991-09-25', 'F', 25000.00, 'Prefers local pickup when available',                         '{"tier":"silver","points":6100}'),
('Tao',    'Huang',  'tao.huang@example.com',      '13300007777', '33 Hubin Road',         'Hangzhou',  'Zhejiang',  '310000', 'China', '1987-04-18', 'M', 60000.00, 'Long-time customer, always pays on time\nReferred 5 friends', '{"tier":"gold","points":18900,"referrals":5}'),
('Ling',   'Wu',     'ling.wu@example.com',        '13200008888', '21 Binjiang Ave',       'Shenzhen',  'Guangdong', '518000', 'China', '1993-12-03', 'F', 10000.00, NULL,                                                           NULL),
('Feng',   'Zhao',   'feng.zhao@example.com',      '13100009999', '8 Heping Road',         'Tianjin',   'Tianjin',   '300000', 'China', '1989-08-07', 'M', 35000.00, 'Interested in home appliances',                               '{"tier":"gold","points":11000}'),
('Qing',   'Sun',    'qing.sun@example.com',       '13000000000', '55 Zhonghua Road',      'Chongqing', 'Chongqing', '400000', 'China', '1994-02-28', 'O', 45000.00, 'New VIP upgrade pending review\nNeeds address verification',   '{"tier":"silver","points":9800,"pending_upgrade":true}');

-- Products
INSERT INTO products (sku, name, category, sub_category, brand, description, specs, price, cost, stock_qty, weight_kg) VALUES
('ELEC-PHONE-001',  'ProMax Ultra 15',         'Electronics', 'Smartphones',    'TechBrand',  'Flagship smartphone with 6.7" OLED display, 256GB storage, 5G capable', '{"screen":"6.7 inch OLED","storage":"256GB","ram":"12GB","battery":"5000mAh","5g":true}', 6999.00, 3500.00, 150, 0.22),
('ELEC-LAPTOP-001', 'AirBook Pro 14',          'Electronics', 'Laptops',        'TechBrand',  'Ultra-thin laptop, M3 chip, 16GB RAM, 512GB SSD, 14" Retina display',  '{"screen":"14 inch Retina","cpu":"M3","ram":"16GB","ssd":"512GB","battery":"18h"}',       9999.00, 6000.00, 80,  1.55),
('ELEC-EAR-001',    'SoundPods Pro',           'Electronics', 'Audio',          'SoundTech',  'Noise-cancelling wireless earbuds with 30h battery life',                '{"driver":"11mm","anc":true,"battery":"30h","bluetooth":"5.3"}',                         1299.00, 400.00,  300, 0.05),
('HOME-CHAIR-001',  'ErgoComfort Office Chair', 'Home',       'Furniture',      'ComfortPlus','Ergonomic mesh office chair with lumbar support and adjustable arms',    '{"material":"mesh","max_weight":"150kg","adjustable":["height","arms","tilt"]}',         2499.00, 800.00,  45,  15.00),
('BOOK-TECH-001',   'Modern Database Systems', 'Books',       'Technology',     'TechPress',  'Comprehensive guide to modern database architecture and optimization',   '{"pages":856,"isbn":"978-7-111-12345-6","language":"zh-CN","edition":3}',                 128.00,  40.00,   500, 1.20),
('CLOTH-JACK-001',  'Urban Windbreaker',       'Clothing',    'Outerwear',      'UrbanWear',  'Waterproof windbreaker jacket, lightweight, packable',                   '{"material":"nylon","waterproof":true,"sizes":["S","M","L","XL","XXL"]}',                599.00,  150.00,  200, 0.35),
('FOOD-TEA-001',    'Dragon Well Green Tea',   'Food',        'Tea',            'TeaMaster',  'Premium Longjing green tea from Hangzhou, 2024 spring harvest',          '{"origin":"Hangzhou","harvest":"2024-spring","weight":"250g","grade":"AAA"}',             388.00,  80.00,   1000, 0.28),
('ELEC-WATCH-001',  'SmartFit Band 7',         'Electronics', 'Wearables',      'FitTech',    'Fitness tracker with heart rate, SpO2, GPS, 14-day battery',             '{"sensors":["hr","spo2","gps","accel"],"battery":"14d","waterproof":"5ATM"}',             699.00,  200.00,  250, 0.03);

-- Orders
INSERT INTO orders (customer_id, order_date, status, shipping_addr, shipping_city, shipping_zip, payment_method, subtotal, tax, shipping_fee, discount, total, tracking_no, notes) VALUES
(1, '2024-12-01 10:30:00', 'delivered',   '88 Nanjing Road, Shanghai',     'Shanghai',  '200001', 'Alipay',     8298.00, 829.80, 0.00,    500.00, 8627.80, 'SF1234567890', 'Gift wrapped'),
(1, '2025-01-15 14:20:00', 'shipped',     '88 Nanjing Road, Shanghai',     'Shanghai',  '200001', 'WeChat Pay', 1299.00, 129.90, 10.00,   0.00,   1438.90, 'YT9876543210', NULL),
(2, '2025-01-20 09:00:00', 'processing',  '66 Wangfujing Street, Beijing', 'Beijing',   '100006', 'Credit Card',9999.00, 999.90, 0.00,    1000.00,9998.90, NULL,            'Urgent - needed by Friday'),
(3, '2025-01-22 16:45:00', 'pending',     '12 Tianhe Road, Guangzhou',     'Guangzhou', '510620', 'Alipay',     727.00,  72.70,  15.00,   0.00,   814.70,  NULL,            NULL),
(4, '2025-01-10 11:00:00', 'delivered',   '99 Jiefang Road, Wuhan',       'Wuhan',     '430000', 'Bank Transfer',16497.00,1649.70,0.00,  2000.00,16146.70,'SF2468013579', 'Corporate PO #TC-2025-0042\nInvoice required'),
(7, '2025-01-25 08:15:00', 'shipped',     '33 Hubin Road, Hangzhou',       'Hangzhou',  '310000', 'Alipay',     388.00,  38.80,  8.00,    0.00,   434.80,  'ZT1357924680', NULL),
(5, '2025-01-28 20:00:00', 'pending',     '45 Chunxi Road, Chengdu',      'Chengdu',   '610000', 'WeChat Pay', 7698.00, 769.80, 0.00,    300.00, 8167.80, NULL,            'Birthday gift for wife');

-- Order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
(1, 1, 1, 6999.00, 6999.00),
(1, 3, 1, 1299.00, 1299.00),
(2, 3, 1, 1299.00, 1299.00),
(3, 2, 1, 9999.00, 9999.00),
(4, 6, 1, 599.00,  599.00),
(4, 5, 1, 128.00,  128.00),
(5, 1, 2, 6999.00, 13998.00),
(5, 4, 1, 2499.00, 2499.00),
(6, 7, 1, 388.00,  388.00),
(7, 1, 1, 6999.00, 6999.00),
(7, 8, 1, 699.00,  699.00);
