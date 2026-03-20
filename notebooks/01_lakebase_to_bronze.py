# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase to Bronze Pipeline
# MAGIC **cvr_tm_demo**: Reads unprocessed records from Lakebase staging tables and inserts them into Delta bronze tables.
# MAGIC
# MAGIC Staging tables (Lakebase PostgreSQL):
# MAGIC - `staging_complaints` -> `cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints`
# MAGIC - `staging_inspections` -> `cvr_dev_ai_kit.cvr_tm_demo.ePDI_Inspections`
# MAGIC - `staging_deliveries` -> `cvr_dev_ai_kit.cvr_tm_demo.ePOD_Delivery`

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.61.0 psycopg2-binary
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Lakebase Connection

# COMMAND ----------

import psycopg2
import psycopg2.extras
import uuid
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import regexp_replace, col, lit

# Lakebase instance name
INSTANCE_NAME = "cvr-staging-db"
DATABASE_NAME = "cvr_staging"

# Get instance details and generate credential
w = WorkspaceClient()
instance = w.database.get_database_instance(name=INSTANCE_NAME)
cred = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[INSTANCE_NAME])

# Connection parameters
LAKEBASE_HOST = instance.read_write_dns
LAKEBASE_PORT = 5432
current_user = spark.sql("SELECT current_user()").collect()[0][0]
token = cred.token

print(f"Lakebase host: {LAKEBASE_HOST}")
print(f"Database: {DATABASE_NAME}")
print(f"User: {current_user}")

# JDBC config for Spark reads
jdbc_url = f"jdbc:postgresql://{LAKEBASE_HOST}:{LAKEBASE_PORT}/{DATABASE_NAME}?sslmode=require"
jdbc_properties = {
    "user": current_user,
    "password": token,
    "driver": "org.postgresql.Driver"
}

# Test connection
def get_connection():
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=LAKEBASE_PORT,
        dbname=DATABASE_NAME,
        user=current_user,
        password=token,
        sslmode="require"
    )

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT version()")
print(f"Connected: {cur.fetchone()[0]}")
cur.close()
conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper: Mark records as processed

# COMMAND ----------

def mark_as_processed(table_name, id_column, id_list):
    """Mark records as processed in Lakebase staging table."""
    if not id_list:
        return 0
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"UPDATE {table_name} SET processed = TRUE WHERE {id_column} = ANY(%s)",
        (id_list,)
    )
    updated = cur.rowcount
    cur.close()
    conn.close()
    return updated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Staging Complaints -> Bronze Customer_Complaints

# COMMAND ----------

complaints_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT complaint_id, vehicle_id, customer_id, complaint_date, complaint_category, description FROM staging_complaints WHERE processed = FALSE ORDER BY ingested_at ASC) as sc",
    properties=jdbc_properties
)

complaint_count = complaints_df.count()
print(f"Found {complaint_count} unprocessed complaints")

if complaint_count > 0:
    original_ids = [row["complaint_id"] for row in complaints_df.select("complaint_id").collect()]

    # Convert string complaint_id to bigint (extract numeric part: CC-1709012345678 -> 1709012345678)
    bronze_complaints = complaints_df.withColumn(
        "complaint_id",
        regexp_replace(col("complaint_id"), "[^0-9]", "").cast("bigint")
    )

    bronze_complaints.write.mode("append").saveAsTable("cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints")
    print(f"Inserted {complaint_count} complaints into bronze table")

    updated = mark_as_processed("staging_complaints", "complaint_id", original_ids)
    print(f"Marked {updated} complaints as processed")
else:
    print("No unprocessed complaints - skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Staging Inspections -> Bronze ePDI_Inspections

# COMMAND ----------

inspections_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT inspection_id, vehicle_id, checklist_item, status, inspection_date, notes FROM staging_inspections WHERE processed = FALSE ORDER BY ingested_at ASC) as si",
    properties=jdbc_properties
)

inspection_count = inspections_df.count()
print(f"Found {inspection_count} unprocessed inspections")

if inspection_count > 0:
    original_ids = [row["inspection_id"] for row in inspections_df.select("inspection_id").collect()]

    # Bronze ePDI_Inspections: inspection_id(bigint), vehicle_id, dealer_id, inspection_date, checklist_item, status, notes
    bronze_inspections = inspections_df \
        .withColumn("inspection_id", regexp_replace(col("inspection_id"), "[^0-9]", "").cast("bigint")) \
        .withColumn("dealer_id", lit("DLR-000")) \
        .select("inspection_id", "vehicle_id", "dealer_id", "inspection_date", "checklist_item", "status", "notes")

    bronze_inspections.write.mode("append").saveAsTable("cvr_dev_ai_kit.cvr_tm_demo.ePDI_Inspections")
    print(f"Inserted {inspection_count} inspections into bronze table")

    updated = mark_as_processed("staging_inspections", "inspection_id", original_ids)
    print(f"Marked {updated} inspections as processed")
else:
    print("No unprocessed inspections - skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Staging Deliveries -> Bronze ePOD_Delivery

# COMMAND ----------

deliveries_df = spark.read.jdbc(
    url=jdbc_url,
    table="(SELECT delivery_id, vehicle_id, dealer_id, delivery_date, delivery_condition FROM staging_deliveries WHERE processed = FALSE ORDER BY ingested_at ASC) as sd",
    properties=jdbc_properties
)

delivery_count = deliveries_df.count()
print(f"Found {delivery_count} unprocessed deliveries")

if delivery_count > 0:
    original_ids = [row["delivery_id"] for row in deliveries_df.select("delivery_id").collect()]

    # Bronze ePOD_Delivery: delivery_id(bigint), vehicle_id, dealer_id, delivery_date, delivery_condition, customer_signature
    bronze_deliveries = deliveries_df \
        .withColumn("delivery_id", regexp_replace(col("delivery_id"), "[^0-9]", "").cast("bigint")) \
        .withColumn("customer_signature", lit("Signed via ePOD App")) \
        .select("delivery_id", "vehicle_id", "dealer_id", "delivery_date", "delivery_condition", "customer_signature")

    bronze_deliveries.write.mode("append").saveAsTable("cvr_dev_ai_kit.cvr_tm_demo.ePOD_Delivery")
    print(f"Inserted {delivery_count} deliveries into bronze table")

    updated = mark_as_processed("staging_deliveries", "delivery_id", original_ids)
    print(f"Marked {updated} deliveries as processed")
else:
    print("No unprocessed deliveries - skipping")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Lakebase to Bronze Pipeline Complete")
print("=" * 60)
complaints_count = spark.sql("SELECT COUNT(*) as cnt FROM cvr_dev_ai_kit.cvr_tm_demo.Customer_Complaints").collect()[0]["cnt"]
inspections_count = spark.sql("SELECT COUNT(*) as cnt FROM cvr_dev_ai_kit.cvr_tm_demo.ePDI_Inspections").collect()[0]["cnt"]
deliveries_count = spark.sql("SELECT COUNT(*) as cnt FROM cvr_dev_ai_kit.cvr_tm_demo.ePOD_Delivery").collect()[0]["cnt"]
print(f"Bronze Customer_Complaints: {complaints_count} total rows")
print(f"Bronze ePDI_Inspections:    {inspections_count} total rows")
print(f"Bronze ePOD_Delivery:       {deliveries_count} total rows")
