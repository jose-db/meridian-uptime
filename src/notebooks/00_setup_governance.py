# Databricks notebook source
# MAGIC %md
# MAGIC # Meridian Uptime — 00 · Unity Catalog setup & governance primitives
# MAGIC Creates the governed schema, the raw landing Volume, and the **column-mask** and
# MAGIC **row-filter** functions that the Lakeflow pipeline DDL attaches to the PII / plant-scoped
# MAGIC tables. Must run before the pipeline (the pipeline references them fully qualified).

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "meridian")
C, S = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {C}.{S} COMMENT 'Meridian Precision — predictive maintenance for JIT OEM supply (FE Bar build)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {C}.{S}.raw COMMENT 'Landing zone for plant source-system drops (historian, CMMS, MES, HR)'")

# COMMAND ----------

# MAGIC %md **PII mask** — technician phone / e-mail visible only to admins and the HR group.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {C}.{S}.mask_pii(val STRING)
RETURNS STRING
COMMENT 'Masks technician PII unless caller is in admins or meridian_hr'
RETURN CASE
  WHEN is_account_group_member('admins') OR is_member('admins') OR is_account_group_member('meridian_hr') THEN val
  ELSE concat('***', right(val, 4))
END""")

# COMMAND ----------

# MAGIC %md **Plant row filter** — a plant maintenance manager sees only their plant; corporate reliability sees all.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {C}.{S}.plant_filter(plant_id STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: admins / corporate reliability see all plants; meridian_plant_<id> groups see their plant'
RETURN is_account_group_member('admins') OR is_member('admins')
    OR is_account_group_member('meridian_reliability_corp')
    OR is_account_group_member(concat('meridian_plant_', lower(plant_id)))""")

display(spark.sql(f"SHOW USER FUNCTIONS IN {C}.{S}"))
