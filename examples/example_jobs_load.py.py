# Databricks notebook source
import pyspark
from databricks_export.jobs import JobRunsHandler
from delta import *

JobRunsHandler(spark, target_table_location="dbfs:/tmp/sri/jobs_delta_dump_v2",
                host="", token="", 
                buffer_size=1000, workspace_name="") \
                .run()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) FROM delta.`dbfs:/tmp/sri/jobs_delta_dump_v2`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`dbfs:/tmp/sri/jobs_delta_dump_v2`

# COMMAND ----------


