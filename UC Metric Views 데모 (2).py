# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Metric Views 데모
# MAGIC
# MAGIC ## 왜 Metric Views가 중요한가: 비즈니스 문제 🔍
# MAGIC
# MAGIC 비즈니스 분석가와 데이터 소비자들은 전통적인 데이터 아키텍처에서 공통된 문제에 직면합니다:
# MAGIC
# MAGIC 1. **일관되지 않은 지표**: "왜 마케팅 부서는 매출이 120만 달러라고 보고하고, 재무 부서는 130만 달러라고 할까요?"
# MAGIC 2. **유연하지 않은 분석**: "제품별 *그리고* 지역별 판매 데이터를 보고 싶은데, 우리 대시보드는 둘 중 하나만 보여줍니다."
# MAGIC 3. **복잡한 질문에 대한 미해결**: "모바일 구매에서 고가 고객의 연간 성장률은 얼마인가요?"
# MAGIC 4. **끝없는 테이블 생성 요청**: "이번 분석을 위해 또 다른 집계 테이블을 IT팀이 만들어야 해요."
# MAGIC
# MAGIC Unity Catalog Metric Views는 지표 정의와 분석 방식을 분리하는 **시맨틱 계층**을 생성함으로써 이러한 문제를 해결합니다.
# MAGIC
# MAGIC ## 사전 요구사항
# MAGIC
# MAGIC - Unity Catalog가 활성화된 Databricks 작업 공간

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데모 개요
# MAGIC
# MAGIC 소매 분석 예제를 통해 다음 내용을 살펴보겠습니다:
# MAGIC
# MAGIC 1. 메달리온 아키텍처 (브론즈 → 실버 → 골드)
# MAGIC 2. 비즈니스 분석에서 골드 테이블만 사용할 때의 한계점
# MAGIC 3. Metric Views가 골드 테이블 위에 시맨틱 계층을 어떻게 생성하는지
# MAGIC 4. 실제 비즈니스 질문을 Metric Views로 어떻게 해결할 수 있는지

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 설정 및 구성
# MAGIC
# MAGIC 먼저, 카탈로그와 스키마를 설정하겠습니다. 환경에 맞게 카탈로그 이름은 쉽게 변경할 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ========================================================
# MAGIC -- 데모 구성 설정 - 필요에 따라 아래 값을 수정하세요
# MAGIC -- ========================================================
# MAGIC
# MAGIC -- 지정한 카탈로그 생성 및 사용
# MAGIC -- CREATE CATALOG IF NOT EXISTS cla_metrics_demo;
# MAGIC USE CATALOG demo_ykko;
# MAGIC
# MAGIC -- 해당 카탈로그 내에 스키마 생성
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS metrics;
# MAGIC
# MAGIC -- 현재 설정된 카탈로그와 데이터베이스 확인
# MAGIC SELECT current_catalog(), current_database();
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 전통적인 접근 방식: 메달리온 아키텍처
# MAGIC
# MAGIC 데이터 분석에서는 종종 데이터를 구성하기 위해 "메달리온" 아키텍처를 사용합니다:
# MAGIC
# MAGIC ```
# MAGIC [Raw Data] → 브론즈 (원시 데이터) → 실버 (정제된 데이터) → 골드 (비즈니스용 데이터)
# MAGIC ```
# MAGIC
# MAGIC 소매 시나리오에 대한 간단한 버전을 구현해보겠습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 브론즈 계층 생성 (원시 데이터)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 브론즈 계층: 원시 거래 데이터
# MAGIC CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
# MAGIC   transaction_id STRING,
# MAGIC   store_id STRING,
# MAGIC   product_id STRING,
# MAGIC   customer_id STRING,
# MAGIC   quantity INT,
# MAGIC   amount DOUBLE,
# MAGIC   transaction_date TIMESTAMP,
# MAGIC   raw_metadata STRING
# MAGIC );
# MAGIC
# MAGIC -- 브론즈 계층: 원시 상품 데이터
# MAGIC CREATE TABLE IF NOT EXISTS bronze.raw_products (
# MAGIC   product_id STRING,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE,
# MAGIC   product_metadata STRING
# MAGIC );
# MAGIC
# MAGIC -- 브론즈 계층: 원시 매장 데이터
# MAGIC CREATE TABLE IF NOT EXISTS bronze.raw_stores (
# MAGIC   store_id STRING,
# MAGIC   store_name STRING,
# MAGIC   region STRING,
# MAGIC   store_metadata STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 샘플 데이터를 사용하여 브론즈 테이블을 채웁니다
# MAGIC INSERT INTO bronze.raw_transactions VALUES
# MAGIC   ('t001', 's001', 'p001', 'c001', 2, 19.98, '2025-01-01 08:30:00', '{"payment_method":"credit","promotion_code":"NEWYEAR","device":"mobile"}'),
# MAGIC   ('t002', 's001', 'p002', 'c002', 1, 29.99, '2025-01-01 09:45:00', '{"payment_method":"debit","device":"web"}'),
# MAGIC   ('t003', 's002', 'p001', 'c003', 3, 29.97, '2025-01-01 10:15:00', '{"payment_method":"cash","device":"in-store"}'),
# MAGIC   ('t004', 's002', 'p003', 'c001', 1, 49.99, '2025-01-01 11:20:00', '{"payment_method":"credit","promotion_code":"NEWYEAR","device":"mobile"}'),
# MAGIC   ('t005', 's001', 'p002', 'c004', 2, 59.98, '2025-01-02 08:10:00', '{"payment_method":"credit","device":"web"}'),
# MAGIC   ('t006', 's003', 'p003', 'c002', 1, 49.99, '2025-01-02 09:30:00', '{"payment_method":"gift_card","device":"in-store"}'),
# MAGIC   ('t007', 's001', 'p001', 'c003', 4, 39.96, '2025-01-02 14:20:00', '{"payment_method":"credit","device":"mobile"}'),
# MAGIC   ('t008', 's002', 'p004', 'c005', 1, 99.99, '2025-01-02 16:45:00', '{"payment_method":"paypal","device":"web"}'),
# MAGIC   ('t009', 's003', 'p002', 'c001', 2, 59.98, '2025-01-03 10:30:00', '{"payment_method":"credit","promotion_code":"WEEKEND","device":"mobile"}'),
# MAGIC   ('t010', 's001', 'p004', 'c004', 1, 99.99, '2025-01-03 13:15:00', '{"payment_method":"debit","device":"web"}');
# MAGIC
# MAGIC INSERT INTO bronze.raw_products VALUES
# MAGIC   ('p001', 'T-Shirt Basic', 'Apparel', 9.99, '{"color":"multi","size":"S-XL","material":"cotton","supplier_id":"sup123"}'),
# MAGIC   ('p002', 'Jeans Classic', 'Apparel', 29.99, '{"color":"blue","size":"28-36","material":"denim","supplier_id":"sup456"}'),
# MAGIC   ('p003', 'Running Shoes', 'Footwear', 49.99, '{"color":"black","size":"6-12","material":"synthetic","supplier_id":"sup789"}'),
# MAGIC   ('p004', 'Smart Watch', 'Electronics', 99.99, '{"color":"silver","connectivity":"bluetooth","waterproof":true,"supplier_id":"sup101"}');
# MAGIC
# MAGIC INSERT INTO bronze.raw_stores VALUES
# MAGIC   ('s001', 'Downtown Store', 'East', '{"address":"123 Main St","sqft":2500,"opening_hours":"9AM-9PM","manager_id":"m101"}'),
# MAGIC   ('s002', 'Mall Location', 'West', '{"address":"456 Market Ave","sqft":1800,"opening_hours":"10AM-8PM","manager_id":"m102"}'),
# MAGIC   ('s003', 'Airport Shop', 'South', '{"address":"789 Airport Blvd","sqft":800,"opening_hours":"7AM-10PM","manager_id":"m103"}');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 실버 계층 생성 (정제된 데이터)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 실버 계층: JSON 추출을 통해 향상된 거래 데이터
# MAGIC CREATE TABLE IF NOT EXISTS silver.transactions AS
# MAGIC SELECT
# MAGIC   transaction_id,
# MAGIC   store_id,
# MAGIC   product_id,
# MAGIC   customer_id,
# MAGIC   quantity,
# MAGIC   amount,
# MAGIC   transaction_date,
# MAGIC   DATE(transaction_date) AS transaction_day,
# MAGIC   get_json_object(raw_metadata, '$.payment_method') AS payment_method,
# MAGIC   get_json_object(raw_metadata, '$.promotion_code') AS promotion_code,
# MAGIC   get_json_object(raw_metadata, '$.device') AS purchase_device
# MAGIC FROM bronze.raw_transactions;
# MAGIC
# MAGIC -- 실버 계층: JSON 추출을 통해 향상된 상품 데이터
# MAGIC CREATE TABLE IF NOT EXISTS silver.products AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price,
# MAGIC   get_json_object(product_metadata, '$.color') AS color,
# MAGIC   get_json_object(product_metadata, '$.size') AS size,
# MAGIC   get_json_object(product_metadata, '$.material') AS material,
# MAGIC   get_json_object(product_metadata, '$.supplier_id') AS supplier_id
# MAGIC FROM bronze.raw_products;
# MAGIC
# MAGIC -- 실버 계층: JSON 추출을 통해 향상된 매장 데이터
# MAGIC CREATE TABLE IF NOT EXISTS silver.stores AS
# MAGIC SELECT
# MAGIC   store_id,
# MAGIC   store_name,
# MAGIC   region,
# MAGIC   get_json_object(store_metadata, '$.address') AS address,
# MAGIC   cast(get_json_object(store_metadata, '$.sqft') AS INT) AS square_feet,
# MAGIC   get_json_object(store_metadata, '$.opening_hours') AS opening_hours,
# MAGIC   get_json_object(store_metadata, '$.manager_id') AS manager_id
# MAGIC FROM bronze.raw_stores;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 구매 행동을 기반으로 한 간단한 고객 세분화 테이블 생성
# MAGIC CREATE TABLE IF NOT EXISTS silver.customer_segments AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   CASE 
# MAGIC     WHEN total_spend >= 100 THEN 'High Value'
# MAGIC     WHEN total_spend >= 50 THEN 'Medium Value'
# MAGIC     ELSE 'Low Value'
# MAGIC   END AS value_segment
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     SUM(amount) AS total_spend
# MAGIC   FROM silver.transactions
# MAGIC   GROUP BY customer_id
# MAGIC );
# MAGIC
# MAGIC -- 향상된 실버 테이블을 확인해보세요
# MAGIC SELECT * FROM silver.transactions LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 골드 계층 생성 (사전 집계된 비즈니스 데이터)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 비즈니스 친화적인 필드와 일부 기술적인 필드가 혼합된 골드 테이블 재생성
# MAGIC -- 기존 테이블 삭제
# MAGIC drop table if exists gold.retail_sales;
# MAGIC
# MAGIC -- 대부분은 이해하기 쉬운 필드명을 사용하되, 일부는 기술적인 이름을 포함한 골드 테이블 생성
# MAGIC CREATE TABLE IF NOT EXISTS gold.retail_sales AS
# MAGIC SELECT
# MAGIC   DATE(t.transaction_date) AS date,
# MAGIC   s.store_name,
# MAGIC   s.region,
# MAGIC   p.category,
# MAGIC   p.product_name,
# MAGIC   t.payment_method,
# MAGIC   t.purchase_device AS device_type,
# MAGIC   -- 기술적인 필드명
# MAGIC   cs.value_segment AS cust_segment,
# MAGIC   -- 축약된 기술 필드명
# MAGIC   -- 사전 계산된 지표들
# MAGIC   COUNT(DISTINCT t.transaction_id) AS transaction_count,
# MAGIC   SUM(t.quantity) AS quantity_sold,
# MAGIC   SUM(t.amount) AS revenue
# MAGIC FROM
# MAGIC   silver.transactions t
# MAGIC   JOIN silver.stores s ON t.store_id = s.store_id
# MAGIC   JOIN silver.products p ON t.product_id = p.product_id
# MAGIC   JOIN silver.customer_segments cs ON t.customer_id = cs.customer_id
# MAGIC GROUP BY
# MAGIC   ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 골드 테이블만으로의 한계
# MAGIC
# MAGIC 골드 테이블 `retail_sales`는 사전 집계된 데이터를 포함하고 있지만, 비즈니스 사용자에게는 다음과 같은 한계가 있습니다:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 문제 1: 일관되지 않은 지표 정의
# MAGIC
# MAGIC 각 팀이 동일한 지표를 서로 다른 방식으로 계산할 수 있습니다:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 개선이 필요한 기술적인 SQL 예시
# MAGIC
# MAGIC SELECT 
# MAGIC   date, 
# MAGIC   store_name, 
# MAGIC   region,
# MAGIC   cust_segment,
# MAGIC   -- device_type을 매번 직접 해석해야 함
# MAGIC   CASE 
# MAGIC     WHEN device_type = 'mobile' THEN 'Mobile App'
# MAGIC     WHEN device_type = 'web' THEN 'Website'
# MAGIC     WHEN device_type = 'in-store' THEN 'In-Store'
# MAGIC     ELSE 'Other'
# MAGIC   END AS device,
# MAGIC   SUM(revenue) AS total_revenue,
# MAGIC   SUM(transaction_count) AS num_transactions,
# MAGIC   SUM(revenue) / SUM(transaction_count) AS avg_order_value
# MAGIC FROM gold.retail_sales
# MAGIC WHERE date BETWEEN '2025-01-01' AND '2025-01-03'
# MAGIC GROUP BY date, store_name, region, cust_segment, device_type
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. device_type에 대한 CASE 문을 모든 쿼리에 반복해야 함
# MAGIC -- 2. cust_segment는 축약형이라 비즈니스 사용자에게 친숙하지 않음
# MAGIC -- 3. avg_order_value 같은 지표 계산이 표준화되어 있지 않음

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 지표 계산이 일관되지 않은 또 다른 예시
# MAGIC SELECT 
# MAGIC   store_name,
# MAGIC   region,
# MAGIC   cust_segment,
# MAGIC   SUM(revenue) AS total_sales,
# MAGIC   COUNT(DISTINCT CONCAT(date, store_name, category)) AS estimated_transactions,
# MAGIC   SUM(revenue) / COUNT(DISTINCT CONCAT(date, store_name, category)) AS estimated_aov
# MAGIC FROM gold.retail_sales
# MAGIC GROUP BY store_name, region, cust_segment;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 이 쿼리는 이전 쿼리와는 다른 방식으로 transaction 수를 계산하려고 하며,
# MAGIC -- 이로 인해 리포트 간 지표 불일치 문제가 발생할 수 있음을 강조

# COMMAND ----------

# MAGIC %md
# MAGIC ### 문제 2: 유연하지 않은 차원 분석
# MAGIC
# MAGIC 비즈니스 사용자가 사전에 정의되지 않은 방식으로 데이터를 분석하고 싶다면 어떻게 될까요?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 구매 채널과 고객 세그먼트별로 데이터를 분석해봅시다
# MAGIC
# MAGIC SELECT
# MAGIC   category,
# MAGIC   -- 비즈니스 용어로 코드 값을 반복적으로 변환
# MAGIC   CASE 
# MAGIC     WHEN device_type = 'mobile' THEN 'Mobile App'
# MAGIC     WHEN device_type = 'web' THEN 'Website'
# MAGIC     WHEN device_type = 'in-store' THEN 'In-Store'
# MAGIC     ELSE 'Other'
# MAGIC   END AS channel,
# MAGIC   -- 또 다른 반복적인 변환
# MAGIC   CASE
# MAGIC     WHEN cust_segment = 'High Value' THEN 'Premium'
# MAGIC     WHEN cust_segment = 'Medium Value' THEN 'Regular'
# MAGIC     WHEN cust_segment = 'Low Value' THEN 'Occasional'
# MAGIC     ELSE 'Unknown'
# MAGIC   END AS customer_type,
# MAGIC   SUM(revenue) AS total_revenue,
# MAGIC   SUM(quantity_sold) AS total_units
# MAGIC FROM gold.retail_sales
# MAGIC GROUP BY category, device_type, cust_segment
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. 모든 분석가가 device_type과 cust_segment를 해석하는 방식을 외워야 함
# MAGIC -- 2. 서로 다른 분석가가 서로 다르게 해석할 수 있어 혼란을 초래함
# MAGIC -- 3. 이러한 변환을 모든 쿼리마다 반복해야 함
# MAGIC -- 4. 가장 중요한 점: 이 작업은 이후에 볼 Metric View 버전에서는 훨씬 간단해짐
# MAGIC --    예: 
# MAGIC --    SELECT `Category`, `Sales Channel`, `Customer Segment`, MEASURE(`Revenue`)
# MAGIC --    FROM metrics.retail_metrics
# MAGIC --    GROUP BY `Category`, `Sales Channel`, `Customer Segment`;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 문제 3: 고급 시계열 분석 부재
# MAGIC
# MAGIC 시간에 따른 누적 지표 계산은 복잡하고 오류가 발생하기 쉽습니다:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 시간 기반 지표 계산에는 복잡한 SQL 윈도우 함수가 필요합니다
# MAGIC WITH daily_revenue AS (
# MAGIC   SELECT 
# MAGIC     date, 
# MAGIC     SUM(revenue) AS day_revenue
# MAGIC   FROM gold.retail_sales
# MAGIC   GROUP BY date
# MAGIC   ORDER BY date
# MAGIC ),
# MAGIC running_total AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     day_revenue,
# MAGIC     SUM(day_revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
# MAGIC   FROM daily_revenue
# MAGIC )
# MAGIC SELECT * FROM running_total;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. 이런 윈도우 함수는 SQL 전문 지식이 필요함
# MAGIC -- 2. 일반적인 비즈니스 사용자는 이런 쿼리를 작성할 수 없음
# MAGIC -- 3. 분석가마다 구현 방식이 달라질 수 있음
# MAGIC -- 4. 가장 중요한 점: 이후에 볼 Metric View 버전에서는 아래와 같이 훨씬 간단함
# MAGIC --    SELECT `Date`, MEASURE(`Revenue`) AS daily_revenue, MEASURE(`Cumulative Revenue`) AS running_total
# MAGIC --    FROM metrics.retail_metrics
# MAGIC --    GROUP BY `Date`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 해결책: Unity Catalog Metric Views를 시맨틱 계층으로 추가
# MAGIC
# MAGIC Metric Views는 골드 테이블 위에 위치한 시맨틱 계층을 제공합니다:
# MAGIC
# MAGIC ```
# MAGIC 브론즈 → 실버 → 골드 → Metric Views (시맨틱 계층)
# MAGIC ```
# MAGIC
# MAGIC 이 시맨틱 계층은 비즈니스 지표를 한 번 정의하고, 유연한 분석을 가능하게 해줍니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 기존 Metric View가 존재하면 삭제
# MAGIC DROP VIEW IF EXISTS metrics.retail_metrics;
# MAGIC
# MAGIC -- 비즈니스 친화적인 차원과 지표를 포함한 Metric View 생성
# MAGIC CREATE VIEW metrics.retail_metrics
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC COMMENT '소매 지표 데모'
# MAGIC AS $$
# MAGIC version: 0.1
# MAGIC source: select * from gold.retail_sales
# MAGIC
# MAGIC # 비즈니스 친화적인 차원 정의
# MAGIC dimensions:
# MAGIC   - name: Date
# MAGIC     expr: date
# MAGIC   - name: Store
# MAGIC     expr: store_name
# MAGIC   - name: Region
# MAGIC     expr: region
# MAGIC   - name: Category
# MAGIC     expr: category
# MAGIC   - name: Product
# MAGIC     expr: product_name
# MAGIC   - name: Payment Method
# MAGIC     expr: payment_method
# MAGIC   # 기술적인 device_type을 비즈니스 용어로 변환
# MAGIC   - name: Sales Channel
# MAGIC     expr: CASE
# MAGIC             WHEN device_type = 'mobile' THEN 'Mobile App'
# MAGIC             WHEN device_type = 'web' THEN 'Website' 
# MAGIC             WHEN device_type = 'in-store' THEN 'In-Store'
# MAGIC             ELSE 'Other Channel'
# MAGIC           END
# MAGIC   # 축약된 cust_segment를 친숙한 용어로 변환
# MAGIC   - name: Customer Segment
# MAGIC     expr: CASE
# MAGIC             WHEN cust_segment = 'High Value' THEN 'Premium Customer'
# MAGIC             WHEN cust_segment = 'Medium Value' THEN 'Regular Customer'
# MAGIC             WHEN cust_segment = 'Low Value' THEN 'Occasional Shopper'
# MAGIC             ELSE 'Unknown'
# MAGIC           END
# MAGIC   # 원본 데이터에 없는 파생 차원 추가
# MAGIC   - name: Day of Week
# MAGIC     expr: date_format(date, 'EEEE')
# MAGIC   # 또 다른 유용한 파생 차원
# MAGIC   - name: Is Weekend
# MAGIC     expr: CASE
# MAGIC             WHEN date_format(date, 'E') IN ('Sat', 'Sun') THEN 'Weekend'
# MAGIC             ELSE 'Weekday'
# MAGIC           END
# MAGIC
# MAGIC # 표준화된 비즈니스 지표 정의
# MAGIC measures:
# MAGIC   - name: Orders
# MAGIC     expr: SUM(transaction_count)
# MAGIC   - name: Units Sold
# MAGIC     expr: SUM(quantity_sold)
# MAGIC   - name: Revenue
# MAGIC     expr: SUM(revenue)
# MAGIC   - name: Average Order Value
# MAGIC     expr: SUM(revenue) / SUM(transaction_count)
# MAGIC   - name: Revenue per Unit
# MAGIC     expr: SUM(revenue) / SUM(quantity_sold)
# MAGIC   # 시간 기반 분석을 위한 누적 지표 정의
# MAGIC   - name: Cumulative Revenue
# MAGIC     expr: SUM(revenue)
# MAGIC     window:
# MAGIC       - order: Date
# MAGIC         range: cumulative
# MAGIC         semiadditive: last
# MAGIC $$;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Metric Views를 활용한 비즈니스 분석
# MAGIC
# MAGIC 이제 Metric Views가 기술적인 SQL을 어떻게 비즈니스 친화적인 분석으로 바꿔주는지 살펴보겠습니다:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 예시 1: 비즈니스 용어로의 전환
# MAGIC
# MAGIC #### 기존 방식: 반복적인 CASE 문이 포함된 골드 테이블 쿼리

# COMMAND ----------

# MAGIC %sql
# MAGIC -- METRIC VIEW 없이: 반복적인 변환 로직이 포함된 복잡한 쿼리
# MAGIC SELECT
# MAGIC   category,
# MAGIC   -- 코드 값을 비즈니스 용어로 반복 변환
# MAGIC   CASE 
# MAGIC     WHEN device_type = 'mobile' THEN 'Mobile App'
# MAGIC     WHEN device_type = 'web' THEN 'Website'
# MAGIC     WHEN device_type = 'in-store' THEN 'In-Store'
# MAGIC     ELSE 'Other'
# MAGIC   END AS channel,
# MAGIC   -- 또 다른 반복적인 변환
# MAGIC   CASE
# MAGIC     WHEN cust_segment = 'High Value' THEN 'Premium'
# MAGIC     WHEN cust_segment = 'Medium Value' THEN 'Regular'
# MAGIC     WHEN cust_segment = 'Low Value' THEN 'Occasional'
# MAGIC     ELSE 'Unknown'
# MAGIC   END AS customer_type,
# MAGIC   SUM(revenue) AS total_revenue
# MAGIC FROM gold.retail_sales
# MAGIC GROUP BY category, device_type, cust_segment
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. 모든 분석가가 device_type과 cust_segment를 해석하는 방식을 기억해야 함
# MAGIC -- 2. 서로 다른 분석가가 다르게 해석할 경우 혼란이 발생할 수 있음
# MAGIC -- 3. 이러한 변환을 모든 쿼리에서 반복해야 함

# COMMAND ----------

# MAGIC %md
# MAGIC #### 이후: Metric View를 통한 내장된 비즈니스 용어 변환

# COMMAND ----------

# MAGIC %sql
# MAGIC -- METRIC VIEW 사용 시: 깔끔하고 비즈니스 친화적인 쿼리
# MAGIC SELECT 
# MAGIC   `Category`,
# MAGIC   `Sales Channel`,
# MAGIC --  `Customer Segment`,
# MAGIC   MEASURE(`Revenue`) AS revenue
# MAGIC FROM metrics.retail_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY revenue DESC;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. device_type이나 cust_segment에 대한 CASE 문이 더 이상 필요하지 않음
# MAGIC -- 2. 모두가 이해할 수 있는 비즈니스 친화적인 차원 이름 사용
# MAGIC -- 3. 변환 로직은 Metric View 정의 안에 한 번만 작성하면 됨 (모든 쿼리에서 반복하지 않음)
# MAGIC -- 4. 모든 팀이 동일한 변환 로직을 사용하므로 일관성 확보 가능

# COMMAND ----------

# MAGIC %sql
# MAGIC -- METRIC VIEW 사용 시: 깔끔하고 비즈니스 친화적인 쿼리
# MAGIC SELECT 
# MAGIC   `Category`,
# MAGIC   `Sales Channel`,
# MAGIC  `Customer Segment`,
# MAGIC   MEASURE(`Revenue`) AS revenue
# MAGIC FROM metrics.retail_metrics
# MAGIC GROUP BY ALL
# MAGIC ORDER BY revenue DESC;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. device_type이나 cust_segment에 대한 CASE 문이 더 이상 필요하지 않음
# MAGIC -- 2. 모두가 이해할 수 있는 비즈니스 친화적인 차원 이름 사용
# MAGIC -- 3. 변환 로직은 Metric View 정의 안에 한 번만 작성하면 됨 (모든 쿼리에서 반복하지 않음)
# MAGIC -- 4. 모든 팀이 동일한 변환 로직을 사용하므로 일관성 확보 가능

# COMMAND ----------

# MAGIC %md
# MAGIC ### 예시 2: 시간 기반 분석
# MAGIC
# MAGIC #### 기존 방식: 복잡한 윈도우 함수 필요

# COMMAND ----------

# MAGIC %sql
# MAGIC -- METRIC VIEW 없이: 복잡한 윈도우 함수가 필요한 경우
# MAGIC WITH daily_revenue AS (
# MAGIC   SELECT 
# MAGIC     date, 
# MAGIC     SUM(revenue) AS day_revenue
# MAGIC   FROM gold.retail_sales
# MAGIC   GROUP BY date
# MAGIC   ORDER BY date
# MAGIC ),
# MAGIC running_total AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     day_revenue,
# MAGIC     SUM(day_revenue) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue
# MAGIC   FROM daily_revenue
# MAGIC )
# MAGIC SELECT * FROM running_total;
# MAGIC
# MAGIC -- 주요 참고 사항:
# MAGIC -- 1. 이러한 윈도우 함수는 SQL 전문 지식이 필요함
# MAGIC -- 2. 일반적인 비즈니스 사용자는 이런 쿼리를 작성하기 어려움
# MAGIC -- 3. 분석가마다 구현 방식이 다를 수 있어 결과가 일관되지 않을 수 있음

# COMMAND ----------

# MAGIC %md
# MAGIC #### 이후: Metric View를 통한 내장된 윈도우 정의를 단순하게 사용

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   date,
# MAGIC   MEASURE(`Revenue`) AS revenue,
# MAGIC   MEASURE(`Cumulative Revenue`) AS cumulative_revenue
# MAGIC FROM
# MAGIC   metrics.retail_metrics
# MAGIC GROUP BY
# MAGIC   ALL
# MAGIC ORDER BY
# MAGIC   1 ;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Unity Catalog Metric Views 도입의 주요 이점
# MAGIC
# MAGIC Unity Catalog Metric Views는 골드 테이블 위에 시맨틱 계층을 추가하여 다음과 같은 핵심 이점을 제공합니다:
# MAGIC
# MAGIC 1. **비즈니스 친화적인 용어**: 기술적인 필드명을 모두가 이해할 수 있는 비즈니스 용어로 변환
# MAGIC
# MAGIC 2. **파생 비즈니스 차원**: 원본 데이터에 존재하지 않는 "요일" 등의 유용한 차원을 생성
# MAGIC
# MAGIC 3. **단일 진실의 원천(Single Source of Truth)**: 한 번 정의한 지표를 조직 전체에서 일관되게 사용 가능
# MAGIC
# MAGIC 4. **고급 분석의 단순화**: 복잡한 SQL 없이도 시간 기반 계산 지원
# MAGIC
# MAGIC 5. **비즈니스 사용자를 위한 셀프 서비스**: SQL 지식 없이도 데이터 탐색 가능
# MAGIC
# MAGIC 6. **깔끔한 아키텍처**: 메달리온 아키텍처의 자연스러운 흐름을 따름 (브론즈 → 실버 → 골드 → 시맨틱)
# MAGIC
# MAGIC 7. **관심사의 분리**: 물리적 데이터 최적화(골드 테이블)와 비즈니스 의미 정의(Metric Views)를 분리

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 주요 참고 사항 및 Q&A
# MAGIC
# MAGIC ### UC Metric Views 특징
# MAGIC
# MAGIC - Metric Views는 **기술 용어를 비즈니스 언어로 변환**하여 누구나 데이터를 이해하고 활용할 수 있게 해줍니다.
# MAGIC - 지표는 **한 번만 정의**하고, 다양한 방식으로 **분석이 가능**합니다.
# MAGIC - 비즈니스 사용자가 **기술 지식 없이도 셀프 서비스** 분석을 수행할 수 있습니다.
# MAGIC - 메달리온 아키텍처에서 **골드 테이블 위에 자연스럽게 위치**하는 구조입니다.
# MAGIC
# MAGIC ### 자주 묻는 질문
# MAGIC
# MAGIC **Q: 기존 View와 무엇이 다른가요?**  
# MAGIC A: 일반 View는 기술 필드를 비즈니스 개념으로 변환하지 않습니다. 또한 동적 집계나 측정 지표 개념이 없어 여전히 group by 등을 직접 정의해야 합니다.
# MAGIC
# MAGIC **Q: Metric Views는 골드 테이블 위에 구축해야 하나요, 실버 테이블도 가능한가요?**  
# MAGIC A: 베스트 프랙티스는 골드 테이블 위에 구축하는 것입니다. 골드 테이블은 이미 비즈니스용 집계와 데이터 품질 검증을 거친 상태이기 때문입니다.
# MAGIC
# MAGIC **Q: 쿼리 시점에서 사용자 정의 계산도 가능한가요?**  
# MAGIC A: 네, `MEASURE()` 함수와 사용자 계산식을 함께 사용할 수 있습니다. 예: `MEASURE(Revenue) / MEASURE(Orders) * 100`
# MAGIC
# MAGIC **Q: 보안과 권한 관리는 어떻게 하나요?**  
# MAGIC A: Metric Views는 Unity Catalog의 보안 모델과 통합되어 있습니다. 지표(Metric)에만 접근 권한을 부여하고, 기반 테이블에 대한 접근은 제한할 수 있습니다.
