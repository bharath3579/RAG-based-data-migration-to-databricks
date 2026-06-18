WITH EmployeeHierarchy AS (
    SELECT
        employee_id,
        employee_name,
        manager_id,
        1 AS hierarchy_level
    FROM employee
    WHERE manager_id IS NULL
    UNION ALL
    SELECT
        e.employee_id,
        e.employee_name,
        e.manager_id,
        eh.hierarchy_level + 1
    FROM employee e
    INNER JOIN EmployeeHierarchy eh
        ON e.manager_id = eh.employee_id
)
SELECT
    employee_id,
    employee_name,
    manager_id,
    hierarchy_level
FROM EmployeeHierarchy
ORDER BY hierarchy_level, employee_id;