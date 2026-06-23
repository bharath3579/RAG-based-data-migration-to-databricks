WITH CustomerSales AS (
    SELECT
        c.customer_id,
        c.customer_name,
        SUM(s.sales_amount) AS total_sales
    FROM customer c
    INNER JOIN sales s
        ON c.customer_id = s.customer_id
    WHERE c.status = 'ACTIVE'
    GROUP BY
        c.customer_id,
        c.customer_name
)
SELECT TOP 10
    customer_id,
    customer_name,
    total_sales
FROM CustomerSales
WHERE total_sales > 1000
ORDER BY total_sales DESC;