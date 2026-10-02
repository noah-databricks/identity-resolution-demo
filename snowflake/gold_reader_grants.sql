-- Run in Databricks SQL. Lets one service principal read the Gold tables from
-- Snowflake through the Unity Catalog Iceberg REST catalog, and nothing else.
-- Replace <catalog> and <application-id> (the service principal's application ID).
GRANT USE CATALOG ON CATALOG <catalog> TO `<application-id>`;
GRANT EXTERNAL USE SCHEMA ON SCHEMA <catalog>.gold TO `<application-id>`;
GRANT USE SCHEMA ON SCHEMA <catalog>.gold TO `<application-id>`;
GRANT SELECT ON SCHEMA <catalog>.gold TO `<application-id>`;
