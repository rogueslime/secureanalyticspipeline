CREATE DATABASE IF NOT EXISTS anl;

CREATE TABLE IF NOT EXISTS anl.dim_date (
  date_key      INT PRIMARY KEY,           -- YYYYMMDD
  `date`        DATE NOT NULL,
  `year`        INT NOT NULL,
  `month`       INT NOT NULL,
  `day`         INT NOT NULL,
  `dow`         TINYINT NOT NULL,          -- 1=Sun..7=Sat (or adjust)
  UNIQUE KEY uk_dim_date_date (`date`)
);

CREATE TABLE IF NOT EXISTS anl.dim_customer (
  customer_key     BIGINT AUTO_INCREMENT PRIMARY KEY,
  user_hash        CHAR(64) NOT NULL UNIQUE,   -- SHA2(...,256) hex
  email_token      CHAR(64),                   -- SHA2(email,256) hex (or token)
  age_bin          VARCHAR(10),
  city             VARCHAR(100),
  state            VARCHAR(50),
  zipcode_prefix   VARCHAR(10),
  signup_date_key  INT,
  CONSTRAINT fk_dim_customer_signup_date
    FOREIGN KEY (signup_date_key) REFERENCES anl.dim_date(date_key)
);

CREATE TABLE IF NOT EXISTS anl.dim_product (
  product_key   BIGINT AUTO_INCREMENT PRIMARY KEY,
  product_id    CHAR(36) NOT NULL UNIQUE,
  sku           VARCHAR(64) NOT NULL,
  `name`        VARCHAR(200) NOT NULL,
  category      VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS anl.fact_order_line (
  order_id         CHAR(36) NOT NULL,
  date_key         INT NOT NULL,
  customer_key     BIGINT NOT NULL,
  product_key      BIGINT NOT NULL,
  quantity         INT NOT NULL,
  revenue_cents    INT NOT NULL,
  tax_cents        INT NOT NULL,
  discount_cents   INT NOT NULL,
  shipping_cents   INT NOT NULL,
  status           ENUM('PLACED','PAID','SHIPPED','CANCELLED') NOT NULL,

  PRIMARY KEY (order_id, product_key),

  CONSTRAINT fk_fact_date
    FOREIGN KEY (date_key) REFERENCES anl.dim_date(date_key),
  CONSTRAINT fk_fact_customer
    FOREIGN KEY (customer_key) REFERENCES anl.dim_customer(customer_key),
  CONSTRAINT fk_fact_product
    FOREIGN KEY (product_key) REFERENCES anl.dim_product(product_key)
);

CREATE INDEX idx_fact_date      ON anl.fact_order_line(date_key);
CREATE INDEX idx_fact_customer  ON anl.fact_order_line(customer_key);
CREATE INDEX idx_fact_product   ON anl.fact_order_line(product_key);
