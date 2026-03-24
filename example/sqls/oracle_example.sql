-- dialect: oracle
-- # Oracle PL/SQL Test File
-- Test: completions for Oracle functions (NVL, NVL2, DECODE, TO_CHAR, LISTAGG)
-- Test: hover over TRUNC, REGEXP_SUBSTR, ADD_MONTHS
-- Test: parameter hints — type NVL( and see (expr1, expr2)
-- Test: go-to-definition — Ctrl+click "employees" to jump to CREATE TABLE
-- Test: snippets — type "plsql" or "proc" for PL/SQL blocks

CREATE TABLE employees (
  emp_id NUMBER PRIMARY KEY,
  first_name VARCHAR2(50),
  last_name VARCHAR2(50),
  hire_date DATE,
  salary NUMBER(10,2),
  dept_id NUMBER,
  manager_id NUMBER,
  email VARCHAR2(100)
);

CREATE TABLE departments (
  dept_id NUMBER PRIMARY KEY,
  dept_name VARCHAR2(100),
  location VARCHAR2(100)
);

-- Try hovering over these functions:
SELECT
  e.emp_id,
  e.first_name || ' ' || e.last_name AS full_name,
  NVL(e.email, 'no-email@company.com') AS email,
  NVL2(e.manager_id, 'Has Manager', 'Top Level') AS mgr_status,
  DECODE(e.dept_id, 10, 'Engineering', 20, 'Sales', 'Other') AS dept_label,
  TO_CHAR(e.hire_date, 'YYYY-MM-DD') AS hired,
  TRUNC(MONTHS_BETWEEN(SYSDATE, e.hire_date) / 12) AS years_employed,
  ADD_MONTHS(e.hire_date, 12) AS first_anniversary,
  LISTAGG(e.first_name, ', ') WITHIN GROUP (ORDER BY e.first_name)
    OVER (PARTITION BY e.dept_id) AS dept_members
FROM employees e
WHERE e.salary BETWEEN 50000 AND 150000
  AND REGEXP_LIKE(e.email, '^[a-z]+\.[a-z]+@')
ORDER BY e.hire_date DESC;

-- Try typing:
-- REGEXP_S  → should suggest REGEXP_SUBSTR, REGEXP_REPLACE
-- SYS_CO    → should suggest SYS_CONTEXT, SYS_CONNECT_BY_PATH
-- LAST_     → should suggest LAST_DAY

-- PL/SQL snippet test — type "plsql" and tab:
