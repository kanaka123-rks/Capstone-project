create database RFM_PROJECT;
USE RFM_PROJECT;
-- Dimension: Customers
CREATE TABLE dim_customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    full_name   VARCHAR(100),
    email       VARCHAR(100),
    phone       VARCHAR(20),
    city        VARCHAR(50),
    state       VARCHAR(50),
    country     VARCHAR(50)
);

-- Dimension: Products
CREATE TABLE dim_products (
    product_id        VARCHAR(20) PRIMARY KEY,
    product_category  VARCHAR(50)
);


-- Fact: Orders
CREATE TABLE fact_orders (
    order_id        VARCHAR(20) PRIMARY KEY,
    customer_id     VARCHAR(20),
    product_id      VARCHAR(20),
    order_date      DATE,
    quantity        INT,
    amount          DECIMAL(10,2),
    channel         VARCHAR(50),
    payment_method  VARCHAR(50),
    order_status    VARCHAR(50),
    
    -- Foreign Keys
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    CONSTRAINT fk_product  FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);
-------------------------------------------------------------------------------------------------------------------------------------
# rows chekking
SELECT DISTINCT customer_id 
FROM fact_orders
WHERE customer_id not IN (SELECT customer_id FROM dim_customers);

select distinct product_id 
from fact_orders
where product_id not in (select  product_id from dim_products);
--------------------------------------------------------------------------------------------------------------------------------------
# null checking 
SELECT COUNT(*) AS total_null_rows
FROM dim_customers
WHERE full_name IS NULL 
   OR email IS NULL
   OR phone IS NULL
   or city is null
   or state is null
   or country is null;
   
select count(*) as total_null_values
from dim_products
where  product_category is null;

select count(*) as total_null_values
from fact_orders 
where order_date is null
     or quantity is null
     or amount is null
     or channel is null
     or payment_method is null
     or order_status is null;
--------------------------------------------------------------------------------------------------------------------------------------
alter	table fact_orders add total_amount decimal(10,2);     
    select * from fact_orders;
UPDATE fact_orders
SET quantity = 0
WHERE quantity = -1;
update fact_orders set total_amount= quantity*amount;


update dim_customers set email='unknownemail@gmail.com' where email='';

UPDATE dim_customers
SET state = CONCAT(UCASE(LEFT(state, 1)), LCASE(SUBSTRING(state, 2)));

update dim_customers set state=trim(state); 

update fact_orders SET payment_method='Upi'where payment_method='upi';
update fact_orders SET payment_method='Credit card'where payment_method='credit card';
update fact_orders set payment_method='COD' WHERE payment_method='Cash on Delivery';
SET SQL_SAFE_UPDATES = 0;

#================================================================================================================================
-- ONE-SHOT: Final RFM with correct handling of Recency, Frequency, Monetary

    -- STEP 1: Create final RFM table
CREATE TABLE rfm_final AS
WITH
-- 1) Aggregate per customer using ONLY Completed orders
customer_orders AS (
    SELECT
        o.customer_id,
        -- Recency: last Completed order; if none, set 999
        CASE
            WHEN MAX(CASE WHEN o.order_status = 'Completed'
                          THEN CAST(o.order_date AS DATE) END) IS NULL
            THEN 999
            ELSE DATEDIFF(
                    CURDATE(),
                    MAX(CASE WHEN o.order_status = 'Completed'
                             THEN CAST(o.order_date AS DATE) END)
                 )
        END AS recency,

        -- Frequency: count Completed
        SUM(CASE WHEN o.order_status = 'Completed' THEN 1 ELSE 0 END) AS frequency,

        -- Monetary: sum Completed amounts
        SUM(CASE WHEN o.order_status = 'Completed' THEN o.total_amount ELSE 0 END) AS monetary
    FROM fact_orders o
    GROUP BY o.customer_id
),

-- 2) Raw NTILE buckets
tiles AS (
    SELECT
        customer_id,
        recency,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency   ASC) AS r_tile_desc,  -- recency higher days worse
        NTILE(5) OVER (ORDER BY frequency DESC) AS f_tile_asc,   -- low freq worse
        NTILE(5) OVER (ORDER BY monetary  DESC) AS m_tile_asc    -- low spend worse
    FROM customer_orders
),

-- 3) Final scores with adjustments (fix for 0 values)
scored AS (
    SELECT
        customer_id,
        recency,
        frequency,
        monetary,

        -- Recency: smaller days = better (recent purchase)
        CASE WHEN recency = 999 THEN 1 ELSE 6 - r_tile_desc END AS R_score,

        -- Frequency: higher orders = better
        CASE WHEN frequency = 0 THEN 1 ELSE 6 - f_tile_asc END AS F_score,

        -- Monetary: higher spend = better
        CASE WHEN monetary = 0 THEN 1 ELSE 6 - m_tile_asc END AS M_score
    FROM tiles
)

-- 4) Final RFM table with segment
SELECT
    customer_id,
    recency,
    frequency,
    monetary,
    R_score,
    F_score,
    M_score,
    (R_score + F_score + M_score) AS RFM_score,
    CASE
        WHEN frequency = 0 OR monetary = 0 THEN 'Lost'
        WHEN R_score >= 4 AND F_score >= 4 AND M_score >= 4 THEN 'Champion'
        WHEN F_score >= 4 AND R_score >= 3                   THEN 'Loyal'
        WHEN R_score <= 2 AND (F_score >= 3 OR M_score >= 3) THEN 'At Risk'
        ELSE 'Regulars'
    END AS segment
FROM scored;
#----------------------------------------------------------------------------------------------------------------------------
create table rfm_results (select r.customer_id,c.full_name,c.city,
                          c.state,r.recency,r.frequency,r.monetary,
							r.R_score,r.F_score,r.M_score,r.RFM_score,r.segment
                            from rfm_final r
                             join dim_customers c on r.customer_id=c.customer_id);
          


 

#----------------------------------------------------------------------------------------------------------------------------------
################################################ STORED_PROCEDURE  ############################################################

# --------------------------------------TOP 10_RFM_CUSTOMERS-------------------------------------------------------

DELIMITER $$

CREATE PROCEDURE Top10_RFM_Customers()
BEGIN
    

    SELECT 
        customer_id,
        full_name,
        Recency,
        Frequency,
        Monetary,
        RFM_Score
    FROM rfm_results
    ORDER BY RFM_Score DESC
    LIMIT 10;
END $$

DELIMITER ;
drop procedure Top10_RFM_Customers;

#----------------------------------------------PRODUCT_CATEGORY----------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE RFM_By_Category()
BEGIN
    SELECT 
        p.product_category,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS Recency,
        COUNT(o.order_id) AS Frequency,
        SUM(o.amount) AS Monetary
    FROM fact_orders o
    JOIN dim_products p ON o.product_id = p.product_id
    WHERE o.order_status = 'Completed'
    GROUP BY p.product_category;
END $$

DELIMITER ;
DROP PROCEDURE RFM_By_Category;
#------------------------------------------------------RFM_BY_CHANNEL----------------------------------------------------------

DELIMITER $$

CREATE PROCEDURE RFM_By_Channel()
BEGIN
    SELECT 
        o.channel,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS Recency,
        COUNT(o.order_id) AS Frequency,
        SUM(o.amount) AS Monetary
    FROM fact_orders o
    WHERE o.order_status = 'Completed'
    GROUP BY o.channel;
END $$

DELIMITER ;

#-----------------------------------------------HIGHVALUE_PAYMENTMETHOD--------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE HighValue_PaymentMethod()
BEGIN
     SELECT 
        o.payment_method,
        COUNT(DISTINCT o.customer_id) AS CustomerCount
    FROM fact_orders o
    JOIN rfm_results r ON o.customer_id = r.customer_id
    WHERE r.Monetary > 50000
    GROUP BY o.payment_method
    ORDER BY CustomerCount DESC;
END $$

DELIMITER ;


drop procedure HighValue_PaymentMethod;
DROP PROCEDURE IF EXISTS HighValue_PaymentMethod;



#------------------------------------------------------ BAD_CUSTOMER ---------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE Bad_Customers()
BEGIN
    SELECT 
        customer_id,
        SUM(CASE WHEN order_status IN ('Cancelled', 'Returned') THEN 1 ELSE 0 END) AS Cancel_Return_Count,
        COUNT(order_id) AS Total_Orders,
        ROUND(SUM(CASE WHEN order_status IN ('Cancelled', 'Returned') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)
                    AS Cancel_Return_Percent
    FROM fact_orders
    GROUP BY customer_id
    HAVING Cancel_Return_Percent > 30;  -- bad customers = more than 30% cancellations
END $$

DELIMITER ;
#------------------------------------------------RFM_BY_STATE---------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE RFM_By_State()
BEGIN
    SELECT 
        c.state,
        DATEDIFF(CURDATE(), MAX(o.order_date)) AS Recency,
        COUNT(o.order_id) AS Frequency,
        SUM(o.amount) AS Monetary
    FROM fact_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'Completed'
    GROUP BY c.state;
END $$

DELIMITER ; 

#-----------------------------------------RFM_BEFORE_AFTER(CHECK_DATE)-----------------------------------------------
DELIMITER $$

CREATE PROCEDURE RFM_Before_After(IN check_date DATE)
BEGIN
    -- Before date
    SELECT 'Before' AS Period, 
           o.customer_id,
           COUNT(o.order_id) AS Frequency,
           SUM(o.amount) AS Monetary
    FROM fact_orders o
    WHERE o.order_date < check_date
    AND o.order_status = 'Completed'
    GROUP BY o.customer_id;

    -- After date
    SELECT 'After' AS Period, 
           o.customer_id,
           COUNT(o.order_id) AS Frequency,
           SUM(o.amount) AS Monetary
    FROM fact_orders o
    WHERE o.order_date >= check_date
    AND o.order_status = 'Completed'
    GROUP BY o.customer_id;
END $$

DELIMITER ;
#-------------------------------------------Segment Distribution -------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE segment_Distribution()
BEGIN

    select segment,count(*)as customer_count
    from rfm_results
    group by segment;
END$$

DELIMITER ;
    drop procedure segment_Distribution;
#---------------------------------------------Churn_Probability-------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE Churn_Probability()
BEGIN
    SELECT 
        customer_id,
        full_name,
        R_score,
        F_score,
        CASE 
            WHEN R_score = 1 AND F_score <= 2 THEN 'High Churn Risk'
            WHEN R_score = 2 AND F_score <= 3 THEN 'Moderate Churn Risk'
			WHEN R_score >= 3 AND F_score <= 5 THEN 'no'
            ELSE 'Low Risk'
        END AS Churn_Probability
    FROM rfm_results ;
END$$

DELIMITER ;
drop procedure Churn_Probability;
#--------------------------------------------Inactive_Customer_Revenue_Loss-------------------------------------------
DELIMITER $$

CREATE PROCEDURE Inactive_Customer()

BEGIN
    SELECT  customer_id,
			city,
            state,
			Recency,
            Frequency,
            Monetary,
            segment
    FROM rfm_results
    WHERE Recency>=180 and Frequency=1;
END$$

DELIMITER ; 
drop procedure  Inactive_Customer;

#---------------------------------------------------cancelled customers---------------------------------------------------
delimiter $$

create procedure  cancelled_customer()

begin

select c.full_name,c.city,c.state,
       o.order_status,o.quantity,o.total_amount,r.segment
 from fact_orders o
 join dim_customers c
 on   o.customer_id=c.customer_id
 join rfm_results r on o.customer_id=r.customer_id 
where o. order_status='cancelled';

end$$
delimiter ;
drop procedure cancelled_customer;
#------------------------------------------GOOD CUSTOMER----------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE good_customers()

begin 

  select r.customer_id,r.city,r.state,o.order_status,r.RFM_score,sum(o.total_amount),r.segment
from fact_orders o join rfm_results r on o.customer_id=r.customer_id
where o.order_status='completed'and r.RFM_score>=10
group by r.customer_id;

  end$$
  DELIMITER ;

drop procedure good_customers;
#----------------------------------------------------------------------------------------------------
delimiter $$
CREATE PROCEDURE Customer_count_method()
begin 

  select count(customer_id)as customer_count,payment_method from fact_orders
   group by payment_method;
end$$
delimiter ;
#----------------------------------------------------------------------------------------------------
DELIMITER $$

CREATE PROCEDURE Analyze_Seasonal_Trends()
BEGIN
    -- Create temporary table to store aggregated trends
    CREATE TEMPORARY TABLE seasonal_trends AS
    SELECT
        YEAR(order_date) AS purchase_year,
        MONTH(order_date) AS purchase_month,
        CASE 
            WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Autumn'
        END AS season,
        COUNT(DISTINCT customer_id) AS active_customers,
        AVG(DATEDIFF(CURDATE(), MAX(order_date))) AS avg_recency,
        COUNT(order_id) AS total_frequency
    FROM fact_orders
    GROUP BY purchase_year, season, purchase_month
    ORDER BY purchase_year, purchase_month;

    -- Select trends for reporting
    SELECT * FROM seasonal_trends;

    -- Drop temporary table after execution
    DROP TEMPORARY TABLE seasonal_trends;
END$$

DELIMITER ;


drop procedure Analyze_Seasonal_Trends;


DELIMITER $$

CREATE PROCEDURE Analyze_Seasonal_Trends()
BEGIN
    CREATE TEMPORARY TABLE seasonal_trends AS
    SELECT
        YEAR(order_date) AS purchase_year,
        MONTH(order_date) AS purchase_month,
        CASE 
            WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Autumn'
        END AS season,
        COUNT(DISTINCT customer_id) AS active_customers,
        AVG(recency_days) AS avg_recency,
        SUM(order_count) AS total_frequency
    FROM (
        SELECT
            customer_id,
            order_date,
            YEAR(order_date) AS purchase_year,
            MONTH(order_date) AS purchase_month,
            CASE 
                WHEN MONTH(order_date) IN (12, 1, 2) THEN 'Winter'
                WHEN MONTH(order_date) IN (3, 4, 5) THEN 'Spring'
                WHEN MONTH(order_date) IN (6, 7, 8) THEN 'Summer'
                WHEN MONTH(order_date) IN (9, 10, 11) THEN 'Autumn'
            END AS season,
            DATEDIFF(CURDATE(), order_date) AS recency_days,
            1 AS order_count
        FROM fact_orders
    ) AS subquery
    GROUP BY purchase_year, season, purchase_month
    ORDER BY purchase_year, purchase_month;

    -- Return result
    SELECT * FROM seasonal_trends;

    DROP TEMPORARY TABLE seasonal_trends;
END$$

DELIMITER ;


