CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.transactions (
  id BIGSERIAL PRIMARY KEY,
  txn_date DATE NOT NULL,
  merchant TEXT,
  description TEXT,
  amount NUMERIC(12,2) NOT NULL,
  category TEXT,
  src_file TEXT,
  load_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
