-- Week 4: PostgreSQL schema. Runs once, when the postgres-data volume is first created.
-- Re-apply manually: docker compose exec -T postgres psql -U fraud -d fraud < sql/init.sql

-- Cleaned, de-duplicated events. transaction_id is the primary key, so a retry cannot create duplicates.
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id    VARCHAR(32) PRIMARY KEY,
    customer_id       VARCHAR(32)  NOT NULL,
    merchant_id       VARCHAR(32)  NOT NULL,
    step              INT          NOT NULL,
    event_time        TIMESTAMP    NOT NULL,   -- UTC
    ingestion_time    TIMESTAMP,               -- UTC
    amount            NUMERIC(12,2) NOT NULL,
    category          VARCHAR(64)  NOT NULL,
    age_group         VARCHAR(8),
    gender            VARCHAR(8),
    zipcode_ori       VARCHAR(16),
    zipcode_merchant  VARCHAR(16),
    fraud_label       INT,                     -- ground truth, never an ML feature
    data_source       VARCHAR(16)  NOT NULL,
    schema_version    VARCHAR(8)   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transactions_event_time ON transactions (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_customer   ON transactions (customer_id);

-- 1-day tumbling window x category (rows are overwritten with the newest value of the window).
CREATE TABLE IF NOT EXISTS daily_stats (
    window_start  TIMESTAMP     NOT NULL,
    window_end    TIMESTAMP     NOT NULL,
    category      VARCHAR(64)   NOT NULL,
    tx_count      BIGINT        NOT NULL,
    total_amount  NUMERIC(16,2) NOT NULL,
    avg_amount    NUMERIC(14,4) NOT NULL,
    PRIMARY KEY (window_start, category)
);

-- 7-day tumbling window x customer.
CREATE TABLE IF NOT EXISTS customer_week (
    window_start         TIMESTAMP     NOT NULL,
    window_end           TIMESTAMP     NOT NULL,
    customer_id          VARCHAR(32)   NOT NULL,
    tx_count             BIGINT        NOT NULL,
    total_amount         NUMERIC(16,2) NOT NULL,
    avg_amount           NUMERIC(14,4) NOT NULL,
    distinct_categories  INT           NOT NULL,
    PRIMARY KEY (window_start, customer_id)
);

-- Invalid messages. id = md5(raw) so the same bad message is stored once.
CREATE TABLE IF NOT EXISTS dead_letter (
    id           CHAR(32) PRIMARY KEY,
    raw          TEXT NOT NULL,
    received_at  TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

