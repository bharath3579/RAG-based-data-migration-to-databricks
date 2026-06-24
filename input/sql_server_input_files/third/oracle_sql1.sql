CREATE OR REPLACE PROCEDURE GetEmployeeDetails (
    p_dept_id IN NUMBER
)
AS
BEGIN
    FOR rec IN (
        SELECT
            e.employee_id,
            e.first_name,
            e.last_name,
            d.department_name,
            e.salary
        FROM employees e
        INNER JOIN departments d
            ON e.department_id = d.department_id
        WHERE e.department_id = p_dept_id
        ORDER BY e.salary DESC
    )
    LOOP
        DBMS_OUTPUT.PUT_LINE(
            rec.employee_id || ' - ' ||
            rec.first_name || ' ' ||
            rec.last_name || ' - ' ||
            rec.department_name || ' - ' ||
            rec.salary
        );
    END LOOP;
END;
/