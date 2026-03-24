-- dialect: sqlite
-- # SQLite Test File
-- Test: completions for SQLite-specific functions (TYPEOF, IFNULL, IIF, JSON_EXTRACT)
-- Test: hover over COALESCE, GROUP_CONCAT, JULIANDAY
-- Test: parameter hints — type IFNULL( and see (expr, fallback)
-- Test: go-to-definition — Ctrl+click "users" in the SELECT to jump to CREATE TABLE

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  age INTEGER,
  metadata TEXT, -- JSON
  created_at TEXT DEFAULT (DATETIME('now'))
);

CREATE VIEW active_users AS
SELECT id, name, email
FROM users
WHERE age IS NOT NULL;

-- Try hovering over these functions:
SELECT
  TYPEOF(age),
  IFNULL(email, 'no-email'),
  IIF(age > 18, 'adult', 'minor'),
  JSON_EXTRACT(metadata, '$.role'),
  GROUP_CONCAT(name, ', '),
  JULIANDAY('now') - JULIANDAY(created_at) AS days_since
FROM users
WHERE name LIKE 'A%'
ORDER BY created_at DESC
LIMIT 10;

-- Try typing these and watch completions:
-- STRF  → should suggest STRFTIME
-- UNIC  → should suggest UNICODE
-- ZERO  → should suggest ZEROBLOB
