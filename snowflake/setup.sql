-- Idempotent bootstrap for the banking pipeline. Placeholders are substituted by init.py
-- (identifiers are validated first).
CREATE WAREHOUSE IF NOT EXISTS {warehouse}
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;
CREATE DATABASE IF NOT EXISTS {database};
CREATE SCHEMA IF NOT EXISTS {database}.RAW;
CREATE SCHEMA IF NOT EXISTS {database}.ANALYTICS;
-- Each RAW table holds one VARIANT column `v`: the Parquet row as a semi-structured object.
CREATE TABLE IF NOT EXISTS {database}.RAW.customers (v VARIANT);
CREATE TABLE IF NOT EXISTS {database}.RAW.accounts (v VARIANT);
CREATE TABLE IF NOT EXISTS {database}.RAW.transactions (v VARIANT)
