WITH CustomerSales AS (
    SELECT
        c.customer_id,
        c.customer_name,
        c.region_id,
        SUM(s.sales_amount) AS total_sales
    FROM testing_sql_ai.sandbox.customers c
    INNER JOIN testing_sql_ai.sandbox.sales s
        ON c.customer_id = s.customer_id
    WHERE c.status = 'ACTIVE'
    GROUP BY
        c.customer_id,
        c.customer_name,
        c.region_id
),
RegionSummary AS (
    SELECT
        region_id,
        COUNT(*) AS customer_count
    FROM CustomerSales
    GROUP BY region_id
)
SELECT
    cs.customer_id,
    cs.customer_name,
    r.region_name,
    cs.total_sales,
    rs.customer_count
FROM CustomerSales cs
INNER JOIN RegionSummary rs
    ON cs.region_id = rs.region_id
LEFT JOIN testing_sql_ai.sandbox.regions r
    ON cs.region_id = r.region_id
WHERE cs.total_sales > 1000
ORDER BY cs.total_sales DESC
LIMIT 10;