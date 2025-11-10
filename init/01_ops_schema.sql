CREATE DATABASE IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.users (
  user_id        CHAR(36) PRIMARY KEY,
  email          VARCHAR(320) NOT NULL UNIQUE,
  phone_number   VARCHAR(32),
  full_name      VARCHAR(200),
  dob            DATE,
  street_address VARCHAR(255),
  city           VARCHAR(100),
  state          VARCHAR(50),
  zipcode        VARCHAR(20),
  signup_ts      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_login_ts  DATETIME NULL
);

CREATE TABLE IF NOT EXISTS ops.products (
  product_id     CHAR(36) PRIMARY KEY,
  sku            VARCHAR(64) NOT NULL UNIQUE,
  name           VARCHAR(200) NOT NULL,
  category       VARCHAR(100),
  price_cents    INT NOT NULL
);

CREATE TABLE IF NOT EXISTS ops.orders (
  order_id        CHAR(36) PRIMARY KEY,
  user_id         CHAR(36) NOT NULL,
  order_ts        DATETIME NOT NULL,
  status          ENUM('PLACED','PAID','SHIPPED','CANCELLED') NOT NULL,
  subtotal_cents  INT NOT NULL,
  tax_cents       INT NOT NULL,
  shipping_cents  INT NOT NULL,
  discount_cents  INT NOT NULL DEFAULT 0,
  CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES ops.users(user_id)
);

CREATE TABLE IF NOT EXISTS ops.order_items (
  order_id         CHAR(36) NOT NULL,
  product_id       CHAR(36) NOT NULL,
  quantity         INT NOT NULL CHECK (quantity > 0),
  unit_price_cents INT NOT NULL,
  PRIMARY KEY (order_id, product_id),
  CONSTRAINT fk_oi_order   FOREIGN KEY (order_id)  REFERENCES ops.orders(order_id),
  CONSTRAINT fk_oi_product FOREIGN KEY (product_id) REFERENCES ops.products(product_id)
);

CREATE INDEX idx_orders_user ON ops.orders(user_id);
CREATE INDEX idx_orders_ts   ON ops.orders(order_ts);
