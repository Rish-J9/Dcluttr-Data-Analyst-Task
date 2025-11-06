🧩 Dcluttr Data Analyst Task
📌 Objective- Create a derived table blinkit_city_insights integrating data from Blinkit’s SKU-level inventory streams, category mapping, and city mapping tables to estimate quantity sold, sales, and stock metrics per SKU, city, and date.

🧠 Problem Context-
E-commerce platforms rely on accurate inventory and sales insights.
Using Blinkit’s raw datasets, this task focuses on estimating demand and tracking stock movement across cities and categories.

⚙️ Skills & Tools-
SQL (MySQL) – Joins, Window Functions, Aggregations, Subqueries, Indexing
Concepts: Inventory Movement, Mode Calculation, Discount Estimation, Data Normalization

📊 Approach-
Data Setup – Created and indexed base tables (scraping_stream, categories, city_map) and loaded CSVs.
Inventory Movement – Used LAG() to compute changes between time slots → est_qty_sold.
Mapping – Joined with city and category data using CRC32() for city identifiers.
Price & Sales Estimation – Derived SP/MRP modes and computed est_sales_sp, est_sales_mrp.
Metrics Calculated – wt_osa, avg_discount, listed_ds_count, in_stock_ds_count.
Final Output – Populated blinkit_city_insights table at grain: Date × SKU × City.

💼 Deliverables-
Task 1&2.sql → Complete SQL pipeline
output.csv → Final dataset (estimates & metrics)

🔍 Highlights-
Built end-to-end SQL workflow for city-level SKU insights.
Applied window functions and aggregations efficiently with indexing.
Generated scalable, analysis-ready dataset for sales and stock tracking.
