SELECT
    e.employee_name,
    d.department_name
FROM employees e,
     departments d
WHERE e.department_id = d.department_id(+);