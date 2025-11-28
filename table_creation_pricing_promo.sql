-- Create DB
CREATE DATABASE IF NOT EXISTS olist_analytics DEFAULT CHARACTER SET utf8mb4;
USE olist_analytics;





-- table creation for orders
CREATE TABLE IF NOT EXISTS olist_orders (
  order_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50),
  order_status VARCHAR(32),
  order_purchase_timestamp DATETIME NULL,
  order_approved_at DATETIME NULL,
  order_delivered_carrier_date DATETIME NULL,
  order_delivered_customer_date DATETIME NULL,
  order_estimated_delivery_date DATETIME NULL
);

-- import the dataset and handle null values
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE olist_orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(order_id, customer_id, order_status,
 @purchase, @approved, @carrier, @customer, @delivery)
SET
  order_purchase_timestamp      = STR_TO_DATE(NULLIF(@purchase,''),'%Y-%m-%d %H:%i:%s'),
  order_approved_at             = STR_TO_DATE(NULLIF(@approved,''),'%Y-%m-%d %H:%i:%s'),
  order_delivered_carrier_date  = STR_TO_DATE(NULLIF(@carrier,''),'%Y-%m-%d %H:%i:%s'),
  order_delivered_customer_date = STR_TO_DATE(NULLIF(@customer,''),'%Y-%m-%d %H:%i:%s'),
  order_estimated_delivery_date = STR_TO_DATE(NULLIF(@delivery,''),'%Y-%m-%d %H:%i:%s');
  
SELECT COUNT(*) FROM olist_orders;





-- table creation for order items
CREATE TABLE IF NOT EXISTS olist_order_items (
  order_id VARCHAR(50),
  order_item_id INT,
  product_id VARCHAR(50),
  seller_id VARCHAR(50),
  shipping_limit_date DATETIME NULL,
  price DECIMAL(10,2),
  freight_value DECIMAL(10,2),
  PRIMARY KEY (order_id, order_item_id)
);

-- import for order items
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE olist_order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(order_id, order_item_id, product_id, seller_id, @shipping, price, freight_value)
SET shipping_limit_date = STR_TO_DATE(NULLIF(@shipping,''), '%Y-%m-%d %H:%i:%s');

SELECT COUNT(*) FROM olist_order_items;





-- table creation for order payments
CREATE TABLE IF NOT EXISTS olist_order_payments (
  order_id VARCHAR(50),
  payment_sequential INT,
  payment_type VARCHAR(32),
  payment_installments INT,
  payment_value DECIMAL(10,2),
  PRIMARY KEY (order_id, payment_sequential)
);

-- import for order payment
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_payments_dataset.csv'
INTO TABLE olist_order_payments
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(order_id, payment_sequential, payment_type, payment_installments, payment_value);

SELECT COUNT(*) FROM olist_order_payments;





-- table creation for products
CREATE TABLE IF NOT EXISTS olist_products (
  product_id VARCHAR(50) PRIMARY KEY,
  product_category_name VARCHAR(128),
  product_name_length INT NULL,
  product_description_length INT NULL,
  product_photos_qty INT NULL,
  product_weight_g INT NULL,
  product_length_cm INT NULL,
  product_height_cm INT NULL,
  product_width_cm INT NULL
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
INTO TABLE olist_products
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(product_id, product_category_name,
 @name, @description, @photos, @weight, @length, @height, @width)
SET
  product_name_length         = NULLIF(@name,''),
  product_description_length  = NULLIF(@description,''),
  product_photos_qty          = NULLIF(@photos,''),
  product_weight_g            = NULLIF(@weight,''),
  product_length_cm           = NULLIF(@length,''),
  product_height_cm           = NULLIF(@height,''),
  product_width_cm            = NULLIF(@width,'');

SELECT COUNT(*) FROM olist_products;





-- customers T.C.
CREATE TABLE IF NOT EXISTS olist_customers (
  customer_id VARCHAR(50) PRIMARY KEY,
  customer_unique_id VARCHAR(50),
  customer_zip_code_prefix INT NULL,
  customer_city VARCHAR(128),
  customer_state VARCHAR(8)
);

-- import for customers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE olist_customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(customer_id, customer_unique_id, @zip, customer_city, customer_state)
SET customer_zip_code_prefix = NULLIF(@zip,'');

SELECT COUNT(*) FROM olist_customers;





-- T.C. for sellers
CREATE TABLE IF NOT EXISTS olist_sellers (
  seller_id VARCHAR(50) PRIMARY KEY,
  seller_zip_code_prefix INT NULL,
  seller_city VARCHAR(128),
  seller_state VARCHAR(8)
);

-- import for sellers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_sellers_dataset.csv'
INTO TABLE olist_sellers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
IGNORE 1 LINES
(seller_id, @zip, seller_city, seller_state)
SET seller_zip_code_prefix = NULLIF(@zip,'');

SELECT COUNT(*) FROM olist_sellers;





-- T.C. for reviews
CREATE TABLE olist_order_reviews (
  review_id VARCHAR(50),
  order_id VARCHAR(50),
  review_score INT,
  review_comment_title TEXT,
  review_comment_message TEXT,
  review_creation_date DATETIME NULL,
  review_answer_timestamp DATETIME NULL
);

-- import for reviews
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_reviews_dataset.csv'
INTO TABLE olist_order_reviews
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(review_id, order_id, review_score, review_comment_title, review_comment_message,
 review_creation_date, review_answer_timestamp);
 
SELECT COUNT(*) FROM olist_order_reviews;