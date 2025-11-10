CREATE OR REPLACE VIEW anl.dim_customer_v AS
SELECT
  SHA2(CONCAT('STATIC_SALT:', u.user_id), 256) AS user_hash,
  SHA2(u.email, 256)                            AS email_token,
  CASE
    WHEN u.dob IS NULL OR u.dob > CURDATE() THEN NULL
    WHEN TIMESTAMPDIFF(YEAR, u.dob, CURDATE()) < 18 THEN '<18'
    WHEN TIMESTAMPDIFF(YEAR, u.dob, CURDATE()) BETWEEN 18 AND 24 THEN '18-24'
    WHEN TIMESTAMPDIFF(YEAR, u.dob, CURDATE()) BETWEEN 25 AND 34 THEN '25-34'
    WHEN TIMESTAMPDIFF(YEAR, u.dob, CURDATE()) BETWEEN 35 AND 44 THEN '35-44'
    WHEN TIMESTAMPDIFF(YEAR, u.dob, CURDATE()) BETWEEN 45 AND 64 THEN '45-64'
    ELSE '65+'
  END AS age_bin,
  u.city, u.state,
  LEFT(COALESCE(u.zipcode,''), 3)               AS zipcode_prefix,
  CAST(DATE_FORMAT(u.signup_ts, '%Y%m%d') AS UNSIGNED) AS signup_date_key
FROM ops.users u;

CREATE OR REPLACE VIEW anl.dim_product_v AS
SELECT p.product_id, p.sku, p.name, p.category
FROM ops.products p;

CREATE OR REPLACE VIEW anl.fact_order_line_v AS
SELECT
  oi.order_id,
  CAST(DATE_FORMAT(o.order_ts, '%Y%m%d') AS UNSIGNED) AS date_key,
  SHA2(CONCAT('STATIC_SALT:', o.user_id), 256)        AS user_hash,
  oi.product_id,
  oi.quantity,
  (oi.quantity * oi.unit_price_cents)                 AS revenue_cents,
  o.tax_cents, o.discount_cents, o.shipping_cents, o.status
FROM ops.order_items oi
JOIN ops.orders o ON o.order_id = oi.order_id;