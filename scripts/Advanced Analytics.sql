
-- Change Over Time Analysis
-- Analyse sales performance over time
select 
YEAR(order_date) as order_year,
MONTH(order_date) as order_month,
SUM(sales_amount) as total_sales,
count(distinct customer_key) as total_customers,
SUM(quantity) as total_quantity
from gold.fact_sales 
where order_date is not null
group by YEAR(order_date) , MONTH(order_date)
order by YEAR(order_date) , MONTH(order_date) ;


-- Calculate the total sales per month 
-- and the running total of sales over time 

select 
order_date, total_sales,
sum(total_sales) over (partition by order_date order by order_date) as running_total_sales
from (
select 
DATETRUNC(MONTH , order_date) as order_date,
sum(sales_amount) as total_sales 
from gold.fact_sales
where order_date is not null
group by DATETRUNC(MONTH , order_date) 
) as t


/*
Performance Analysis (Year-over-Year, Month-over-Month)
  - Analyze the yearly performance of products by comparing their sales
	to both the average sales performance of the product and the previous year's sales */

with yearly_product_sales as(
select 
	year(order_date) as order_year,
	p.product_name,
	SUM(f.sales_amount) as current_sales
from gold.fact_sales f left join gold.dim_products p
	on f.product_key = p.product_key
where order_date is not null 
group by year(order_date) ,p.product_name
)
select 
order_year, 
product_name , 
current_sales,
AVG(current_sales) over (partition by product_name) as AVG_Sales,
current_sales - AVG(current_sales) over (partition by product_name) as AVG_DIFF,
case when current_sales - AVG(current_sales) over (partition by product_name) > 0 then 'Above AVG'
	 when current_sales - AVG(current_sales) over (partition by product_name) < 0 then 'Below AVG'
	 else 'AVG'
end as AVG_Change,
-- Year-over-Year Analysis
lag(current_sales) over (partition by product_name order by order_year) as previous_year_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE 
     WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
     WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
     ELSE 'No Change'
END AS py_change
from yearly_product_sales
group by order_year, product_name, current_sales ;


-- Which categories contribute the most to overall sales?
WITH category_sales AS (
    SELECT
        p.category,
        SUM(f.sales_amount) AS total_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON p.product_key = f.product_key
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    SUM(total_sales) OVER () AS overall_sales,
    ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER ()) * 100, 2) AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;

/*Segment products into cost ranges and 
count how many products fall into each segment*/
WITH product_segments AS (
    SELECT
        product_key,
        product_name,
        cost,
        CASE 
            WHEN cost < 100 THEN 'Below 100'
            WHEN cost BETWEEN 100 AND 500 THEN '100-500'
            WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
            ELSE 'Above 1000'
        END AS cost_range
    FROM gold.dim_products
)
SELECT 
    cost_range,
    COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC;

/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than €5,000.
	- Regular: Customers with at least 12 months of history but spending €5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/
WITH customer_spending AS (
    SELECT
        c.customer_key,
        SUM(f.sales_amount) AS total_spending,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON f.customer_key = c.customer_key
    GROUP BY c.customer_key
)
SELECT 
    customer_segment,
    COUNT(customer_key) AS total_customers
FROM (
    SELECT 
        customer_key,
        CASE 
            WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
            WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment
    FROM customer_spending
) AS segmented_customers
GROUP BY customer_segment
ORDER BY total_customers DESC;


