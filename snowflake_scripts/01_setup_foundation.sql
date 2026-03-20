-- ============================================================
-- HealthFlow 360 — Snowflake Foundation Setup
-- Run this script as ACCOUNTADMIN
-- ============================================================

-- ── CONCEPT: Warehouses are compute, not storage ─────────────
-- In Snowflake, storage and compute are SEPARATED.
-- A Virtual Warehouse is pure compute — it spins up in seconds,
-- you pay only when it's running, and it auto-suspends when idle.
-- This is fundamentally different from traditional databases
-- where compute and storage are always tied together.

-- Step 1: Create Virtual Warehouse
CREATE WAREHOUSE IF NOT EXISTS HEALTHFLOW_WH
    WAREHOUSE_SIZE    = 'X-SMALL'    -- Enough for 11.5M rows
    AUTO_SUSPEND      = 60           -- Suspend after 60s idle
    AUTO_RESUME       = TRUE         -- Wake up automatically on query
    INITIALLY_SUSPENDED = TRUE       -- Don't run until needed
    COMMENT = 'HealthFlow 360 Capstone Warehouse';

-- Step 2: Create Database
CREATE DATABASE IF NOT EXISTS HEALTHFLOW_DB
    COMMENT = 'HealthFlow 360 Healthcare Data Platform';

-- Step 3: Use the database
USE DATABASE HEALTHFLOW_DB;

-- Step 4: Create the 3 schemas — Bronze, Silver, Gold
-- CONCEPT: Schema = logical namespace to organize objects
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze Layer — raw as-is data from source systems';

CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Silver Layer — cleansed, typed, validated data';

CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Gold Layer — Star Schema for reporting & analytics';

-- Step 5: Create roles for access control
-- CONCEPT: RBAC — Role Based Access Control
-- Nobody uses ACCOUNTADMIN for daily work. You create
-- specific roles with specific permissions.
CREATE ROLE IF NOT EXISTS HEALTHFLOW_ADMIN;
CREATE ROLE IF NOT EXISTS HEALTHFLOW_DEVELOPER;
CREATE ROLE IF NOT EXISTS HEALTHFLOW_ANALYST;

-- Grant role hierarchy
-- CONCEPT: Roles inherit from each other
-- ADMIN > DEVELOPER > ANALYST
GRANT ROLE HEALTHFLOW_DEVELOPER TO ROLE HEALTHFLOW_ADMIN;
GRANT ROLE HEALTHFLOW_ANALYST   TO ROLE HEALTHFLOW_DEVELOPER;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE HEALTHFLOW_WH TO ROLE HEALTHFLOW_ADMIN;
GRANT USAGE ON WAREHOUSE HEALTHFLOW_WH TO ROLE HEALTHFLOW_DEVELOPER;
GRANT USAGE ON WAREHOUSE HEALTHFLOW_WH TO ROLE HEALTHFLOW_ANALYST;

-- Grant database access
GRANT USAGE ON DATABASE HEALTHFLOW_DB TO ROLE HEALTHFLOW_ADMIN;
GRANT USAGE ON DATABASE HEALTHFLOW_DB TO ROLE HEALTHFLOW_DEVELOPER;
GRANT USAGE ON DATABASE HEALTHFLOW_DB TO ROLE HEALTHFLOW_ANALYST;

-- Grant schema-level permissions by role
-- ADMIN: full access to everything
GRANT ALL ON SCHEMA HEALTHFLOW_DB.RAW      TO ROLE HEALTHFLOW_ADMIN;
GRANT ALL ON SCHEMA HEALTHFLOW_DB.STAGING  TO ROLE HEALTHFLOW_ADMIN;
GRANT ALL ON SCHEMA HEALTHFLOW_DB.ANALYTICS TO ROLE HEALTHFLOW_ADMIN;

-- DEVELOPER: can read RAW, write STAGING & ANALYTICS
GRANT USAGE  ON SCHEMA HEALTHFLOW_DB.RAW       TO ROLE HEALTHFLOW_DEVELOPER;
GRANT ALL    ON SCHEMA HEALTHFLOW_DB.STAGING   TO ROLE HEALTHFLOW_DEVELOPER;
GRANT ALL    ON SCHEMA HEALTHFLOW_DB.ANALYTICS TO ROLE HEALTHFLOW_DEVELOPER;

-- ANALYST: read-only on ANALYTICS only
GRANT USAGE  ON SCHEMA HEALTHFLOW_DB.ANALYTICS TO ROLE HEALTHFLOW_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA HEALTHFLOW_DB.ANALYTICS
    TO ROLE HEALTHFLOW_ANALYST;

-- Grant roles to your user
GRANT ROLE HEALTHFLOW_ADMIN     TO USER KAMESH63;
GRANT ROLE HEALTHFLOW_DEVELOPER TO USER KAMESH63;
GRANT ROLE HEALTHFLOW_ANALYST   TO USER KAMESH63;

SELECT CURRENT_USER();

-- Step 6: Verify everything was created
SHOW WAREHOUSES   LIKE 'HEALTHFLOW%';
SHOW DATABASES    LIKE 'HEALTHFLOW%';
SHOW SCHEMAS      IN DATABASE HEALTHFLOW_DB;
SHOW ROLES        LIKE 'HEALTHFLOW%';