-- ============================================================
-- Service Principal Permissions for Databricks App
-- Replace <SP_CLIENT_ID> with your app's Service Principal ID
-- Replace <YOUR_CATALOG> / <YOUR_SCHEMA> with your values
-- ============================================================

-- Unity Catalog permissions (for gold table reads)
GRANT USE CATALOG ON CATALOG <YOUR_CATALOG> TO `<SP_CLIENT_ID>`;
GRANT USE SCHEMA ON SCHEMA <YOUR_CATALOG>.<YOUR_SCHEMA> TO `<SP_CLIENT_ID>`;
GRANT SELECT ON SCHEMA <YOUR_CATALOG>.<YOUR_SCHEMA> TO `<SP_CLIENT_ID>`;

-- Lakebase catalog permissions (for staging table read/write)
GRANT USE CATALOG ON CATALOG <YOUR_LAKEBASE_CATALOG> TO `<SP_CLIENT_ID>`;
GRANT USE SCHEMA ON SCHEMA <YOUR_LAKEBASE_CATALOG>.public TO `<SP_CLIENT_ID>`;
GRANT SELECT ON SCHEMA <YOUR_LAKEBASE_CATALOG>.public TO `<SP_CLIENT_ID>`;
GRANT ALL PRIVILEGES ON SCHEMA <YOUR_LAKEBASE_CATALOG>.public TO `<SP_CLIENT_ID>`;
