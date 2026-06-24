CREATE OR REPLACE PROCEDURE GetEmployeeDetails(p_dept_id INT)
LANGUAGE SQL
BEGIN
  FOR rec IN (
    SELECT CONCAT(e.employee_id, ' - ', e.first_name, ' ', e.last_name, ' - ', d.department_name, ' - ', e.salary) AS output
    FROM testing_sql_ai.sandbox.employees e
    INNER JOIN testing_sql_ai.sandbox.departments d
      ON e.department_id = d.department_id
    WHERE e.department_id = p_dept_id
    ORDER BY e.salary DESC
  )
  DO
    SELECT rec.output;
  END FOR;
END;

CALL GetEmployeeDetails(1);