MERGE INTO target_customers t
USING source_customers s
ON (t.customer_id = s.customer_id)

WHEN MATCHED THEN
    UPDATE SET
        t.customer_name = s.customer_name,
        t.city = s.city

WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        customer_name,
        city
    )
    VALUES (
        s.customer_id,
        s.customer_name,
        s.city
    );