/* # SAS Test File */
/* Test: completions for SAS functions (CATX, INTNX, INTCK, PRXMATCH) */
/* Test: hover over PUT, INPUT, COMPRESS, SCAN */
/* Test: parameter hints — type INTNX( and see (interval, date, increment, [alignment]) */
/* Test: snippets — type "procsql", "datastep", "macro", "procmeans" */

/* ---- DATA Step example ---- */
DATA work.clean_patients;
  SET raw.patients;

  LENGTH full_name $100;
  full_name = CATX(' ', first_name, last_name);

  age = INTCK('YEAR', birth_date, TODAY());
  next_review = INTNX('MONTH', last_visit, 6, 'S');
  days_since = INTCK('DAY', last_visit, TODAY());

  IF PRXMATCH('/^\d{3}-\d{2}-\d{4}$/', ssn) THEN ssn_valid = 1;
  ELSE ssn_valid = 0;

  phone_clean = COMPRESS(phone, , 'kd');
  state_upper = UPCASE(state);
  name_proper = PROPCASE(full_name);

  visit_month = PUT(last_visit, YYMMN6.);
  income_num = INPUT(income_str, COMMA12.);

  IF age >= 18 AND ssn_valid = 1;
RUN;

/* ---- PROC SQL example ---- */
PROC SQL;
  CREATE TABLE work.summary AS
  SELECT
    department,
    COUNT(*) AS headcount,
    AVG(salary) AS avg_salary,
    MIN(hire_date) AS earliest_hire,
    MAX(hire_date) AS latest_hire,
    CALCULATED avg_salary * 1.1 AS projected_salary
  FROM work.employees
  WHERE status = 'Active'
  GROUP BY department
  HAVING CALCULATED headcount >= 5
  ORDER BY avg_salary DESC;
QUIT;

/* ---- PROC MEANS example ---- */
PROC MEANS DATA=work.clean_patients N MEAN STD MIN MAX;
  VAR age days_since income_num;
RUN;

/* ---- PROC FREQ example ---- */
PROC FREQ DATA=work.clean_patients;
  TABLES state * ssn_valid / CHISQ NOCOL NOROW;
RUN;

/* ---- Macro example ---- */
%MACRO summarize(dsn=, var=, by=);
  PROC MEANS DATA=&dsn NOPRINT;
    VAR &var;
    BY &by;
    OUTPUT OUT=work.stats MEAN= STD= / AUTONAME;
  RUN;
%MEND summarize;

%summarize(dsn=work.clean_patients, var=age income_num, by=state);

/* Try typing these and watch completions: */
/* COALESCEC → SAS-specific character coalesce */
/* IFN       → inline numeric if               */
/* IFC       → inline character if              */
/* DATEPART  → extract date from datetime       */
/* MONOTONIC → row counter in PROC SQL          */
