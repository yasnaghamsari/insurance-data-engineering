-- Local Docker Compose seed for the "policies" source table, read by Bronze
-- over JDBC when SOURCE_MODE=databases (the Airflow DAG's setting).
--
-- This intentionally does NOT reuse setup/sql/mysql/config.sql: that script
-- grants AWS RDS/Fivetran-specific permissions (GRANT ... mysql.rds_kill,
-- a fivetran user) that only make sense against a managed RDS instance and
-- fail on a plain mysql:8 container. It's kept as-is as the cloud/Fivetran
-- provisioning reference; this file is the local-compose equivalent.
--
-- The goal is for Bronze to see exactly what it sees when it reads the CSV
-- directly (SOURCE_MODE=files), so:
--   * column types mirror what Spark infers from the CSV (CUST_ID is a
--     string, the numeric columns DOUBLE, DEDUCTABLE INT);
--   * empty fields load as NULL, not 0 — SUM_INSURED is empty on about half
--     the rows, and silently turning that into 0 would change exposure;
--   * there's no primary key on POLICY_NO. The CSV repeats 7 policy numbers,
--     and deduplicating them is Silver's job (with a documented rule), not
--     something to leave to whichever row MySQL happens to load first.

USE insurance;

CREATE TABLE IF NOT EXISTS policies (
    CUST_ID VARCHAR(64),
    POLICY_NO VARCHAR(128),
    POLICYTYPE VARCHAR(128),
    POL_ISSUE_DATE VARCHAR(128),
    POL_EFF_DATE VARCHAR(128),
    POL_EXPIRY_DATE VARCHAR(128),
    BODY VARCHAR(64),
    MAKE VARCHAR(128),
    MODEL VARCHAR(256),
    MODEL_YEAR DOUBLE,
    CHASSIS_NO VARCHAR(512),
    USE_OF_VEHICLE VARCHAR(128),
    DRV_DOB VARCHAR(128),
    BOROUGH VARCHAR(256),
    NEIGHBORHOOD VARCHAR(512),
    ZIP_CODE DOUBLE,
    PRODUCT VARCHAR(128),
    SUM_INSURED DOUBLE,
    PREMIUM DOUBLE,
    DEDUCTABLE INT
);

-- The sample CSV is mounted into the container's secure-file-priv directory
-- by docker-compose.yml (see the mysql service's volumes). Every field goes
-- through a user variable so empty strings can become NULL; the last one
-- also has any trailing '\r' stripped, in case the file was checked out with
-- CRLF line endings.
LOAD DATA INFILE '/var/lib/mysql-files/policies.csv'
  INTO TABLE policies
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  LINES TERMINATED BY '\n'
  IGNORE 1 ROWS
  (@cust_id, @policy_no, @policytype, @issue, @eff, @expiry, @body, @make, @model, @model_year,
   @chassis, @use, @dob, @borough, @neighborhood, @zip, @product, @sum_insured, @premium, @deductable)
  SET
    CUST_ID = NULLIF(@cust_id, ''),
    POLICY_NO = NULLIF(@policy_no, ''),
    POLICYTYPE = NULLIF(@policytype, ''),
    POL_ISSUE_DATE = NULLIF(@issue, ''),
    POL_EFF_DATE = NULLIF(@eff, ''),
    POL_EXPIRY_DATE = NULLIF(@expiry, ''),
    BODY = NULLIF(@body, ''),
    MAKE = NULLIF(@make, ''),
    MODEL = NULLIF(@model, ''),
    MODEL_YEAR = NULLIF(@model_year, ''),
    CHASSIS_NO = NULLIF(@chassis, ''),
    USE_OF_VEHICLE = NULLIF(@use, ''),
    DRV_DOB = NULLIF(@dob, ''),
    BOROUGH = NULLIF(@borough, ''),
    NEIGHBORHOOD = NULLIF(@neighborhood, ''),
    ZIP_CODE = NULLIF(@zip, ''),
    PRODUCT = NULLIF(@product, ''),
    SUM_INSURED = NULLIF(@sum_insured, ''),
    PREMIUM = NULLIF(@premium, ''),
    DEDUCTABLE = NULLIF(TRIM(TRAILING '\r' FROM @deductable), '');
