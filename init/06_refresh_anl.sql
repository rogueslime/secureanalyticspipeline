-- products → dim_product
INSERT INTO anl.dim_product (product_id, sku, `name`, category)
SELECT v.product_id, v.sku, v.`name`, v.category
FROM anl.dim_product_v v
ON DUPLICATE KEY UPDATE sku=VALUES(sku), `name`=VALUES(`name`), category=VALUES(category);

-- users → dim_customer
INSERT INTO anl.dim_customer (user_hash, email_token, age_bin, city, state, zipcode_prefix, signup_date_key)
SELECT v.user_hash, v.email_token, v.age_bin, v.city, v.state, v.zipcode_prefix, v.signup_date_key
FROM anl.dim_customer_v v
ON DUPLICATE KEY UPDATE
  email_token=VALUES(email_token),
  age_bin=VALUES(age_bin),
  city=VALUES(city),
  state=VALUES(state),
  zipcode_prefix=VALUES(zipcode_prefix),
  signup_date_key=VALUES(signup_date_key);

-- fact: map natural keys to dimension surrogate keys
INSERT INTO anl.fact_order_line
(order_id, date_key, customer_key, product_key, quantity,
 revenue_cents, tax_cents, discount_cents, shipping_cents, status)
SELECT
  f.order_id,
  f.date_key,
  dc.customer_key,
  dp.product_key,
  f.quantity,
  f.revenue_cents,
  f.tax_cents,
  f.discount_cents,
  f.shipping_cents,
  f.status
FROM anl.fact_order_line_v f
JOIN anl.dim_customer dc ON dc.user_hash = f.user_hash
JOIN anl.dim_product  dp ON dp.product_id = f.product_id
ON DUPLICATE KEY UPDATE
  quantity=VALUES(quantity),
  revenue_cents=VALUES(revenue_cents),
  tax_cents=VALUES(tax_cents),
  discount_cents=VALUES(discount_cents),
  shipping_cents=VALUES(shipping_cents),
  status=VALUES(status);
