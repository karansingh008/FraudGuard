-- ============================================================
-- Data Quality Monitoring & Anomaly Detection System
-- Database Schema — Credit Card Transaction Dataset
-- ============================================================

CREATE DATABASE IF NOT EXISTS analytics;
USE analytics;

-- -----------------------------------------------
-- Drop tables in dependency order
-- -----------------------------------------------
DROP TABLE IF EXISTS anomalies;
DROP TABLE IF EXISTS validation_results;
DROP TABLE IF EXISTS data_quality_history;
DROP TABLE IF EXISTS raw_transactions;

-- -----------------------------------------------
-- 1. Raw ingested credit card transactions
-- -----------------------------------------------
CREATE TABLE raw_transactions (
    transaction_id   INT AUTO_INCREMENT PRIMARY KEY,
    time_elapsed     FLOAT,
    v1  FLOAT, v2  FLOAT, v3  FLOAT, v4  FLOAT, v5  FLOAT,
    v6  FLOAT, v7  FLOAT, v8  FLOAT, v9  FLOAT, v10 FLOAT,
    v11 FLOAT, v12 FLOAT, v13 FLOAT, v14 FLOAT, v15 FLOAT,
    v16 FLOAT, v17 FLOAT, v18 FLOAT, v19 FLOAT, v20 FLOAT,
    v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT, v25 FLOAT,
    v26 FLOAT, v27 FLOAT, v28 FLOAT,
    amount           DECIMAL(12, 2),
    class            TINYINT            -- 0 = normal, 1 = fraud (ground truth)
);

-- -----------------------------------------------
-- 2. Per-row validation results
-- -----------------------------------------------
CREATE TABLE validation_results (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id  INT,
    rule_name       VARCHAR(100),
    rule_passed     BOOLEAN,
    FOREIGN KEY (transaction_id) REFERENCES raw_transactions(transaction_id)
);

-- -----------------------------------------------
-- 3. Detected anomalies (ML-based)
-- -----------------------------------------------
CREATE TABLE anomalies (
    id               INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id   INT,
    anomaly_score    FLOAT,
    detection_method VARCHAR(50),
    FOREIGN KEY (transaction_id) REFERENCES raw_transactions(transaction_id)
);

-- -----------------------------------------------
-- 4. Historical data quality scores
-- -----------------------------------------------
CREATE TABLE data_quality_history (
    record_date   DATE,
    quality_score FLOAT,
    total_rows    INT,
    failed_checks INT
);
