-- Task 1 Setting up a local database 

CREATE TABLE all_blinkit_category_scraping_stream (
created_at DATETIME  ,   
l1_category_id INT,   
l2_category_id INT,  
store_id INT,  
sku_id BIGINT, 
sku_name TEXT, 
selling_price DECIMAL(10,2), 
mrp DECIMAL(10,2),  
inventory INT,  
image_url TEXT,   
brand_id INT,   
brand VARCHAR(255),   
unit VARCHAR(50),  
PRIMARY KEY (created_at, sku_id, store_id),                       
INDEX idx_created_at (created_at),                        -- helpful indexes for common queries/joins  
INDEX idx_store (store_id), 
INDEX idx_sku (sku_id)
);

LOAD DATA local INFILE  "C:/Users/risha/Desktop/dcluttr task/raw data csv files/all_blinkit_category_scraping_stream.csv"
INTO TABLE all_blinkit_category_scraping_stream
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;                                                                                                                                       


CREATE TABLE blinkit_categories ( 
l1_category VARCHAR(255) ,
l1_category_id INT ,
l2_category VARCHAR(255) , 
l2_category_id INT , 
PRIMARY KEY (l2_category_id), 
INDEX idx_l1_category_id (l1_category_id),                    -- keep an index on l1 for faster lookups by parent category  
INDEX idx_l2_category (l2_category)
);


CREATE TABLE blinkit_city_map (                                
store_id INT primary key,
city_name VARCHAR(100),
INDEX idx_city_name (city_name)
);                                                           -- data imported using wizard as it's of lesser size


-- TASK 2-------------------------------------------------------------------------------------------

-- 1) Create final table schema explicitly 
CREATE TABLE blinkit_city_insights (
  `date` DATE NOT NULL,
  sku_id BIGINT NOT NULL,
  city_id BIGINT NOT NULL,
  city VARCHAR(255),
  sku_name VARCHAR(512),
  brand_id INT,
  brand VARCHAR(255),
  image_url TEXT,
  category_id INT,
  category_name VARCHAR(255),
  sub_category_id INT,
  sub_category_name VARCHAR(255),

  est_qty_sold BIGINT DEFAULT 0,
  est_sales_sp DECIMAL(14,2) DEFAULT 0.00,
  est_sales_mrp DECIMAL(14,2) DEFAULT 0.00,

  listed_ds_count INT DEFAULT 0,
  ds_count INT DEFAULT 0,
  in_stock_ds_count INT DEFAULT 0,

  wt_osa DECIMAL(8,4) DEFAULT NULL,
  wt_osa_ls DECIMAL(8,4) DEFAULT NULL,

  mrp DECIMAL(12,2) DEFAULT NULL,
  sp DECIMAL(12,2) DEFAULT NULL,

  avg_discount DECIMAL(8,4) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================
-- 2) Step 1: inventory movement (join to city map, compute prev inventory using LAG)
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_inventory_movement;
CREATE TEMPORARY TABLE tmp_inventory_movement AS
SELECT
  DATE(s.created_at) AS date,
  s.created_at,
  s.store_id,
  c.city_name AS city,
  CRC32(c.city_name) AS city_id,
  s.sku_id,
  s.sku_name,
  s.brand_id,
  s.brand,
  s.image_url,
  s.l1_category_id,
  s.l2_category_id,
  s.selling_price,
  s.mrp,
  s.inventory,
  CASE WHEN s.inventory > 0 THEN 1 ELSE 0 END AS in_stock,
  LAG(s.inventory) OVER (PARTITION BY s.store_id, s.sku_id ORDER BY s.created_at) AS prev_inventory
FROM all_blinkit_category_scraping_stream s
JOIN blinkit_city_map c ON s.store_id = c.store_id;

-- Add index to temp table to speed later steps (optional)
ALTER TABLE tmp_inventory_movement ADD INDEX idx_tmp_mv_store_sku (store_id, sku_id, created_at);

-- =====================================================
-- 3) Step 2: estimation (apply your prev-inventory > inventory logic)
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_estimation;
CREATE TEMPORARY TABLE tmp_estimation AS
SELECT
  im.*,
  CASE
    WHEN im.prev_inventory IS NULL THEN 0
    WHEN im.prev_inventory > im.inventory THEN (im.prev_inventory - im.inventory)
    ELSE 0
  END AS est_qty_sold_interval
FROM tmp_inventory_movement im;

ALTER TABLE tmp_estimation ADD INDEX idx_est_sku_date (date, sku_id, city_id);

-- =====================================================
-- 4) Step 3: map categories (LEFT JOIN to keep stream rows even if category missing)
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_category_mapped;
CREATE TEMPORARY TABLE tmp_category_mapped AS
SELECT
  te.*,
  cat.l1_category AS category_name,
  cat.l2_category AS sub_category_name
FROM tmp_estimation te
LEFT JOIN blinkit_categories cat
  ON te.l2_category_id = cat.l2_category_id;

ALTER TABLE tmp_category_mapped ADD INDEX idx_catm_sku_city_date (date, sku_id, city_id);

-- =====================================================
-- 5) Step 4: compute mode for selling_price (sp) and mrp using counts + row_number
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_sp_counts;
CREATE TEMPORARY TABLE tmp_sp_counts AS
SELECT date, sku_id, city_id, selling_price, COUNT(*) AS cnt
FROM tmp_category_mapped
GROUP BY date, sku_id, city_id, selling_price;

DROP TEMPORARY TABLE IF EXISTS tmp_sp_mode;
CREATE TEMPORARY TABLE tmp_sp_mode AS
SELECT date, sku_id, city_id, selling_price AS sp FROM (
  SELECT sc.*,
    ROW_NUMBER() OVER (PARTITION BY sc.date, sc.sku_id, sc.city_id
                       ORDER BY sc.cnt DESC, sc.selling_price DESC) AS rn
  FROM tmp_sp_counts sc
) t WHERE rn = 1;

ALTER TABLE tmp_sp_mode ADD INDEX idx_sp_mode (date, sku_id, city_id);

-- MRP mode
DROP TEMPORARY TABLE IF EXISTS tmp_mrp_counts;
CREATE TEMPORARY TABLE tmp_mrp_counts AS
SELECT date, sku_id, city_id, mrp, COUNT(*) AS cnt
FROM tmp_category_mapped
GROUP BY date, sku_id, city_id, mrp;

DROP TEMPORARY TABLE IF EXISTS tmp_mrp_mode;
CREATE TEMPORARY TABLE tmp_mrp_mode AS
SELECT date, sku_id, city_id, mrp FROM (
  SELECT mc.*,
    ROW_NUMBER() OVER (PARTITION BY mc.date, mc.sku_id, mc.city_id
                       ORDER BY mc.cnt DESC, mc.mrp DESC) AS rn
  FROM tmp_mrp_counts mc
) t WHERE rn = 1;

ALTER TABLE tmp_mrp_mode ADD INDEX idx_mrp_mode (date, sku_id, city_id);

-- =====================================================
-- 6) Step 5: aggregate to the target grain (date, sku_id, city_id)
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_base_aggregated;
CREATE TEMPORARY TABLE tmp_base_aggregated AS
SELECT
  cm.date,
  cm.city_id,
  ANY_VALUE(cm.city)                 AS city,
  cm.sku_id,
  ANY_VALUE(cm.sku_name)             AS sku_name,
  ANY_VALUE(cm.brand_id)             AS brand_id,
  ANY_VALUE(cm.brand)                AS brand,
  ANY_VALUE(cm.image_url)            AS image_url,
  ANY_VALUE(cm.l1_category_id)       AS category_id,
  ANY_VALUE(cm.category_name)        AS category_name,
  ANY_VALUE(cm.l2_category_id)       AS sub_category_id,
  ANY_VALUE(cm.sub_category_name)    AS sub_category_name,

  SUM(cm.est_qty_sold_interval)      AS est_qty_sold,
  COUNT(DISTINCT cm.store_id)        AS listed_ds_count,
  COUNT(DISTINCT CASE WHEN cm.in_stock = 1 THEN cm.store_id END) AS in_stock_ds_count

FROM tmp_category_mapped cm
GROUP BY cm.date, cm.city_id, cm.sku_id;

ALTER TABLE tmp_base_aggregated ADD INDEX idx_base_agg (date, sku_id, city_id);

-- =====================================================
-- 7) Step 6: city totals from blinkit_city_map (ds_count)
-- =====================================================
DROP TEMPORARY TABLE IF EXISTS tmp_city_totals;
CREATE TEMPORARY TABLE tmp_city_totals AS
SELECT CRC32(city_name) AS city_id, city_name, COUNT(*) AS ds_count                     -- crc32 to convert city name to dummy codes  
FROM blinkit_city_map
GROUP BY city_name;

ALTER TABLE tmp_city_totals ADD INDEX idx_city_totals (city_id);

-- =====================================================
-- 8) Step 7: insert deduplicated final rows into blinkit_city_insights
-- =====================================================
INSERT INTO blinkit_city_insights (
  `date`, sku_id, city_id, city, sku_name, brand_id, brand, image_url,
  category_id, category_name, sub_category_id, sub_category_name,
  est_qty_sold, est_sales_sp, est_sales_mrp,
  listed_ds_count, ds_count, in_stock_ds_count,
  wt_osa, wt_osa_ls, mrp, sp, avg_discount
)
SELECT
  ba.date,
  ba.sku_id,
  ba.city_id,
  ba.city,
  ba.sku_name,
  ba.brand_id,
  ba.brand,
  ba.image_url,
  ba.category_id,
  ba.category_name,
  ba.sub_category_id,
  ba.sub_category_name,

  -- estimated sold and sales figures
  COALESCE(ba.est_qty_sold, 0) AS est_qty_sold,

  -- compute est_sales using mode prices (join tmp_sp_mode / tmp_mrp_mode)
  ROUND(COALESCE(ba.est_qty_sold, 0) * COALESCE(spm.sp, 0), 2) AS est_sales_sp,
  ROUND(COALESCE(ba.est_qty_sold, 0) * COALESCE(mrm.mrp, 0), 2) AS est_sales_mrp,

  ba.listed_ds_count,
  COALESCE(ct.ds_count, 0) AS ds_count,
  ba.in_stock_ds_count,

  -- wt_osa and wt_osa_ls
  CASE WHEN COALESCE(ct.ds_count, 0) = 0 THEN NULL ELSE ROUND(1.0 * ba.in_stock_ds_count / ct.ds_count, 4) END AS wt_osa,
  CASE WHEN ba.listed_ds_count = 0 THEN NULL ELSE ROUND(1.0 * ba.in_stock_ds_count / ba.listed_ds_count, 4) END AS wt_osa_ls,

  -- attach mrp and sp from modes
  mrm.mrp,
  spm.sp,

  -- discount avg placeholder (compute per-row as (mrp-sp)/mrp and aggregate avg)
  CASE WHEN mrm.mrp IS NULL OR mrm.mrp = 0 THEN NULL ELSE ROUND((mrm.mrp - spm.sp) / mrm.mrp, 4) END AS avg_discount

FROM tmp_base_aggregated ba
LEFT JOIN tmp_sp_mode spm ON ba.date = spm.date AND ba.sku_id = spm.sku_id AND ba.city_id = spm.city_id
LEFT JOIN tmp_mrp_mode mrm ON ba.date = mrm.date AND ba.sku_id = mrm.sku_id AND ba.city_id = mrm.city_id
LEFT JOIN tmp_city_totals ct ON ba.city_id = ct.city_id;

-- =====================================================
-- 9) Add primary key and indexes to final table
-- =====================================================
ALTER TABLE blinkit_city_insights
  ADD PRIMARY KEY (`date`, `sku_id`, `city_id`);

ALTER TABLE blinkit_city_insights
  ADD INDEX idx_city_sku (city_id, sku_id),
  ADD INDEX idx_date_city (date, city_id);


