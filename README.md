📌 Objective

Build an analytical SQL pipeline to generate a derived table blinkit_city_insights, integrating and analyzing data from Blinkit’s SKU-level inventory streams, category metadata, and city mappings.
The goal is to estimate quantity sold (est_qty_sold) per SKU, per city, per date — and calculate sales, stock availability, and discounts for insights on product performance.

🧠 Problem Context

E-commerce platforms like Blinkit need to understand city-wise inventory movement and sales performance.
Given three base tables:

all_blinkit_category_scraping_stream – SKU-level public data segmented by dark store and date

blinkit_categories – Category hierarchy (L1 & L2)

blinkit_city_map – Maps store IDs to city names

You are required to integrate and analyze these datasets to create actionable insights.

⚙️ Skills & Tools Used

SQL Concepts: Joins, Window Functions, Aggregations, Subqueries, Temporary Tables

Techniques: Inventory Movement Tracking, Data Normalization, Mode Calculation, Discount Analysis

Database: MySQL (InnoDB Engine)

🧾 Approach

Data Setup

Created and indexed base tables for categories, city mapping, and inventory stream.

Loaded raw CSV data using LOAD DATA INFILE and import wizard.

Inventory Movement

Used LAG() window function to compare consecutive inventory values by store and SKU.

Calculated estimated sold quantity (est_qty_sold_interval) where inventory decreased.

Category & City Mapping

Joined with blinkit_categories and blinkit_city_map for contextual labeling.

Generated unique city identifiers using CRC32(city_name).

Sales Estimation

Computed mode of Selling Price (SP) and MRP using row numbering and frequency counts.

Calculated total sales value (est_sales_sp, est_sales_mrp) for each city–SKU–date combination.

Metrics Computed

est_qty_sold → Estimated quantity sold

wt_osa / wt_osa_ls → Weighted On-Shelf Availability

avg_discount → Average discount percentage

in_stock_ds_count, listed_ds_count, ds_count → Store-level coverage metrics

Final Table

Derived table: blinkit_city_insights

Grain: Date × SKU × City

Contains ~20+ columns capturing SKU performance, sales, and availability metrics.

📊 Deliverables

✅ Task 1&2.sql → SQL query to build and populate blinkit_city_insights.

✅ output.csv → Derived dataset output.

✅ Key metrics to support category-level insights and demand forecasting.

💼 Highlights

Built end-to-end SQL data pipeline with 8+ transformation steps.

Applied window functions, aggregations, and joins efficiently using indexes.

Delivered scalable data model for city-level SKU performance tracking.
