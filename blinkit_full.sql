--====================================================================================================================================================================
--====================================================================================================================================================================
CREATE TABLE blinkit_customer_feedback (
  feedback_id bigint,
  order_id bigint,
  customer_id bigint,
  rating bigint,
  feedback_text VARCHAR(255),
  feedback_category VARCHAR(255),
  sentiment VARCHAR(255),
  feedback_date VARCHAR(255)
);

--====================================================================================================================================================================
--====================================================================================================================================================================
CREATE TABLE blinkit_customers (
  customer_id bigint,
  customer_name VARCHAR(255),
  email VARCHAR(255),
  phone bigint,
  address VARCHAR(255),
  area VARCHAR(255),
  pincode bigint,
  registration_date VARCHAR(255),
  customer_segment VARCHAR(255),
  total_orders INT,
  avg_order_value FLOAT
);

--==================================================================================================================================================================== 
--====================================================================================================================================================================
CREATE TABLE blinkit_delivery_performance (
  order_id bigint,
  delivery_partner_id bigint,
  promised_time VARCHAR(255),
  actual_time VARCHAR(255),
  delivery_time_minutes FLOAT,
  distance_km FLOAT,
  delivery_status VARCHAR(255),
  reasons_if_delayed VARCHAR(255)
);

--====================================================================================================================================================================
--==================================================================================================================================================================== 
CREATE TABLE blinkit_inventory (
  product_id bigint,
  date VARCHAR(255),
  stock_received bigint,
  damaged_stock bigint
);

--====================================================================================================================================================================
--====================================================================================================================================================================
CREATE TABLE blinkit_marketing_performance (
  campaign_id bigint,
  campaign_name VARCHAR(255),
  date VARCHAR(255),
  target_audience VARCHAR(255),
  channel VARCHAR(255),
  impressions bigint,
  clicks bigint,
  conversions bigint,
  spend FLOAT,
  revenue_generated FLOAT,
  roas FLOAT
);

--====================================================================================================================================================================
--====================================================================================================================================================================
CREATE TABLE blinkit_order_items (
  order_id bigint,
  product_id bigint,
  quantity bigint,
  unit_price FLOAT
);

--====================================================================================================================================================================
--====================================================================================================================================================================
CREATE TABLE blinkit_orders (
  order_id bigint,
  customer_id bigint,
  order_date VARCHAR(255),
  promised_delivery_time VARCHAR(255),
  actual_delivery_time VARCHAR(255),
  delivery_status VARCHAR(255),
  order_total FLOAT,
  payment_method VARCHAR(255),
  delivery_partner_id bigint,
  store_id bigint
);

select
  *
from
  blinkit_orders --====================================================================================================================================================================
  --====================================================================================================================================================================
  CREATE TABLE blinkit_products (
    product_id bigint,
    product_name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    price FLOAT,
    mrp FLOAT,
    margin_percentage FLOAT,
    shelf_life_days bigint,
    min_stock_level bigint,
    max_stock_level bigint
  );

--====================================================================================================================================================================
--===================================SQl ANALYSIS BASIC TO ADVANCE ===================================================================================================
--===================================================================================================================================================================
-- Q1 Find customers who have placed at least one order but never made a payment.
--===================================================================================================================================================================
select
  c.customer_name
from
  blinkit_orders o
  join blinkit_customers c on o.customer_id = c.customer_id
group by
  c.customer_name
HAVING
  COUNT(o.payment_method) = 0;

-- In this query first we take customer name from blinkit orders table and give the alias name for joining because the payment mode is an another table after taking the customer name with alias 
-- we join the blinkit customer table and then we grouped with customer name and give condition with having clause which is the count of payment method and = 0 .
--===================================================================================================================================================================
-- Q2 Retrieve the second highest priced product.
--===================================================================================================================================================================
select
  product_name,
  price
from
  blinkit_products
order by
  price DESC offset 1 rows FETCH NEXT 1 ROWS ONLY;

-- In this query first we take the requirements product name and price after that we order by the output by price in descending order means which one is high that is on top and
-- we give one condition which is offset 1 rows fetch next 1 rows only this condition is showing that the result which one is after the next one of 1st row. Means 2nd row is the output.
--===================================================================================================================================================================
-- Q3 Find all orders placed in the last 7 days dynamically.
--===================================================================================================================================================================
select
  *
from
  blinkit_orders
where
  order_date >= (
    select
      DATEADD(day, -7, MAX(order_date))
    from
      blinkit_orders
  );

--===================================================================================================================================================================
-- Q4 Identify customers whose names start and end with the same letter.
--===================================================================================================================================================================
select
  *
from
  blinkit_customers
where
  lower(left(customer_name, 1)) = lower(right(customer_name, 1));

--===================================================================================================================================================================
-- Q5 Find duplicate records in the customers table.
--===================================================================================================================================================================
SELECT
  customer_id,
  customer_name,
  area,
  COUNT(*) as totalcount
from
  blinkit_customers
group by
  customer_id,
  customer_name,
  area
HAVING
  COUNT(*) > 1;

--===================================================================================================================================================================
-- Q6 Get orders where total amount is greater than the customers average order value.
--===================================================================================================================================================================
select
  top 3 *
from
  blinkit_orders;

select
  top 3 *
from
  blinkit_customers;

select
  bo.order_id,
  bo.order_total,
  bc.avg_order_value
from
  blinkit_customers bc
  join blinkit_orders bo on bc.customer_id = bo.customer_id
where
  bo.order_total > bc.avg_order_value;

--===================================================================================================================================================================
-- Q7 Find customers whohave not placed any orders.
--===================================================================================================================================================================
select
  bc.customer_id,
  bc.customer_name
from
  blinkit_customers bc
  left join blinkit_orders bo on bc.customer_id = bo.customer_id
where
  bo.order_id is null;

--===================================================================================================================================================================
-- Q8 Retrieve the latest order for each customer.
--===================================================================================================================================================================
select
  customer_id,
  order_id,
  order_date
FROM
  (
    SELECT
      customer_id,
      order_id,
      order_date,
      row_number() over (
        partition by customer_id
        order by
          order_date
      ) as rn
    from
      blinkit_orders
  ) t
where
  rn = 1;

--===================================================================================================================================================================
-- Q9 Find customers who placed orders on consecutive days.
--===================================================================================================================================================================
select
  *
from
  (
    select
      customer_id,
      order_id,
      order_date,
      LAG(order_date) OVER(
        partition by customer_id
        order by
          order_date
      ) as prev_order
    from
      blinkit_orders
  ) t
where
  DATEDIFF(day, prev_order, order_date) = 1;

--===================================================================================================================================================================
-- Q10 Identify customers with missing/null  critical data.
--===================================================================================================================================================================
select
  *
from
  blinkit_customers
where
  email is null
  or phone is null
  or area is null
  or pincode is null;

--===================================================================================================================================================================
-- Q11 Find Total Orders.(For Kpi)
--===================================================================================================================================================================
select
  COUNT(*) as total_orders
from
  blinkit_orders;

--===================================================================================================================================================================
-- Q12 Find Total Customers.(For Kpi)
--===================================================================================================================================================================
select
  COUNT(*) as total_customers
from
  blinkit_customers;

--===================================================================================================================================================================
-- Q13 Find Total Products.(For Kpi)
--===================================================================================================================================================================
select
  COUNT(*) as Total_Products
from
  blinkit_products;

--===================================================================================================================================================================
-- Q14 Find Total Sales(Kpi)
--===================================================================================================================================================================
select
  top 2 *
from
  blinkit_customers
select
  top 2 *
from
  blinkit_orders
select
  SUM(order_total) as total_sales
from
  blinkit_orders;

--===================================================================================================================================================================
-- Q15 Find the month wise customer retention For Kpi)
--===================================================================================================================================================================
WITH customer_orders AS (
  SELECT
    customer_id,
    YEAR(order_date) AS yr,
    MONTH(order_date) AS mn
  FROM
    blinkit_orders
),
retention AS (
  SELECT
    DISTINCT c1.customer_id,
    c1.yr,
    c1.mn
  FROM
    customer_orders c1
    JOIN customer_orders c2 ON c1.customer_id = c2.customer_id
    AND (
      (
        c2.yr = c1.yr
        AND c2.mn = c1.mn + 1
      )
      OR (
        c1.mn = 12
        AND c2.mn = 1
        AND c2.yr = c1.yr + 1
      )
    )
)
SELECT
  yr AS year,
  mn AS month,
  COUNT(DISTINCT customer_id) AS retained_customers
FROM
  retention
GROUP BY
  yr,
  mn
ORDER BY
  yr,
  mn;

--===================================================================================================================================================================
-- Q16 Find the total customer retention in percentage.
--===================================================================================================================================================================
WITH customer_orders AS (
  SELECT
    customer_id,
    YEAR(order_date) AS yr,
    MONTH(order_date) AS mn
  FROM
    blinkit_orders
),
retained AS (
  SELECT
    DISTINCT c1.customer_id
  FROM
    customer_orders c1
    JOIN customer_orders c2 ON c1.customer_id = c2.customer_id
    AND (
      (
        c2.yr = c1.yr
        AND c2.mn = c1.mn + 1
      )
      OR (
        c1.mn = 12
        AND c2.mn = 1
        AND c2.yr = c1.yr + 1
      )
    )
)
SELECT
  (
    COUNT(DISTINCT r.customer_id) * 100.0 / COUNT(DISTINCT c.customer_id)
  ) AS retention_percentage
FROM
  customer_orders c
  LEFT JOIN retained r ON c.customer_id = r.customer_id;

--===================================================================================================================================================================
-- Q17 Find the cancellation rate(For Kpi)
--===================================================================================================================================================================
SELECT
  (
    COUNT(
      CASE
        WHEN delivery_status = 'cancelled' THEN 1
      END
    ) * 100.0 / COUNT(*)
  ) AS cancellation_rate
FROM
  blinkit_orders;

select
  top 2 *
from
  blinkit_customers
select
  top 2 *
from
  blinkit_orders
select
  top 2 *
from
  blinkit_delivery_performance
select
  top 2 *
from
  blinkit_marketing_performance
select
  top 2 *
from
  blinkit_order_items 
  --===================================================================================================================================================================
  -- Q18 Find monthly sales
  --===================================================================================================================================================================
select
  YEAR(order_date) as [year name],
  MONTH(order_date) as monthno,
  DATENAME(month, order_date) as [month name],
  SUM(order_total) as total_sales
from
  blinkit_orders
group by
  YEAR(order_date),
  month(order_date),
  DATENAME(month, order_date)
order by
  [year name],
  monthno;

--===================================================================================================================================================================
-- Q19 Find top 10 customers on the basis of high spent.
--===================================================================================================================================================================
select
  top 10 customer_id,
  SUM(order_total) as total_sales
from
  blinkit_orders
group by
  customer_id
order by
  total_sales desc;

--===================================================================================================================================================================
-- Q20 Create a final dataset.
--===================================================================================================================================================================

CREATE VIEW final_dataset AS
SELECT 
    -- Order Info
    o.order_id,
    o.order_date,
    o.payment_method,
    o.order_total,
    
    -- Customer Info
    c.customer_id,
    c.customer_name,
    c.area,
    c.customer_segment,
    
    -- Product Info
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    
    -- Order Item Info
    oi.quantity,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS item_total,
    
    -- Delivery Info
    d.delivery_time_minutes,
    d.distance_km,
    d.delivery_status,
    
    -- Derived Columns (VERY IMPORTANT)
    
    -- Delay Flag
    CASE 
        WHEN d.delivery_time_minutes > 30 THEN 'Late'
        ELSE 'On Time'
    END AS delivery_flag,
    
    -- Order Month
    SUBSTRING(o.order_date, 1, 7) AS order_month,
    
    -- High Value Order
    CASE 
        WHEN o.order_total > 1000 THEN 'High Value'
        ELSE 'Normal'
    END AS order_type

FROM blinkit_orders o

LEFT JOIN blinkit_customers c 
    ON o.customer_id = c.customer_id

LEFT JOIN blinkit_order_items oi 
    ON o.order_id = oi.order_id

LEFT JOIN blinkit_products p 
    ON oi.product_id = p.product_id

LEFT JOIN blinkit_delivery_performance d 
    ON o.order_id = d.order_id;

    select * from final_dataset;