-- Local Docker Compose seed for the "policies" source table.
--
-- This intentionally does NOT reuse setup/sql/mysql/config.sql: that script
-- grants AWS RDS/Fivetran-specific permissions (GRANT ... mysql.rds_kill,
-- a fivetran user) that only make sense against a managed RDS instance and
-- fail on a plain mysql:8 container. It's kept as-is as the cloud/Fivetran
-- provisioning reference; this file is the local-compose equivalent.

USE insurance;

CREATE TABLE IF NOT EXISTS policies (
    CUST_ID INT,
    POLICY_NO VARCHAR(128) PRIMARY KEY,
    POLICYTYPE VARCHAR(128),
    POL_ISSUE_DATE VARCHAR(128),
    POL_EFF_DATE VARCHAR(128),
    POL_EXPIRY_DATE VARCHAR(128),
    BODY VARCHAR(64),
    MAKE VARCHAR(128),
    MODEL VARCHAR(256),
    MODEL_YEAR FLOAT,
    CHASSIS_NO VARCHAR(512),
    USE_OF_VEHICLE VARCHAR(128),
    DRV_DOB VARCHAR(128),
    BOROUGH VARCHAR(256),
    NEIGHBORHOOD VARCHAR(512),
    ZIP_CODE INT,
    PRODUCT VARCHAR(128),
    SUM_INSURED FLOAT,
    PREMIUM FLOAT,
    DEDUCTABLE FLOAT
);

-- The sample CSV has a handful of rows whose numeric columns don't fit
-- cleanly (e.g. an over-precision SUM_INSURED value), which MySQL 8's
-- default strict sql_mode promotes from a warning to a hard error that
-- aborts the whole LOAD. This is sample-data noise, not something the
-- pipeline depends on being exact, so relax it for this local seed load.
SET SESSION sql_mode = '';

-- The sample CSV also has duplicate POLICY_NO values, which would otherwise
-- abort the whole load against a PRIMARY KEY column. IGNORE INTO TABLE skips
-- the conflicting rows (as a warning) instead of failing the statement.
--
-- The sample CSV is mounted into the container's secure-file-priv directory
-- by docker-compose.yml (see the mysql service's volumes).
LOAD DATA INFILE '/var/lib/mysql-files/policies.csv'
  IGNORE INTO TABLE policies
  FIELDS TERMINATED BY ','
  ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS;
