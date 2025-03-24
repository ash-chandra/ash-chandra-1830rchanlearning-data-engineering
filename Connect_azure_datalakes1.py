# Databricks notebook source
storage_account = 'chandrastorageforlake'
storage_container = 'outputs'
mtcars_storage_filepath = 'output-from-blob/mtcars.parquet'
storage_account_access_key= 'dz8PVeGjXEPqqZchHnF1u1GEC2qawpygtuL3R6gIluA+aqRhkIUOJkzzOogMMT8LT1UrCw0SMFSV+AStcE6Tmg=='

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",f"{storage_account_access_key}"
    )

# COMMAND ----------

mtcars_storage_path = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{mtcars_storage_filepath}"

# COMMAND ----------

spark.read.format('parquet').load(mtcars_storage_path)

# COMMAND ----------

mtcarsdf = spark.read.format("parquet").load(mtcars_storage_path)

# COMMAND ----------

display(mtcarsdf)

# COMMAND ----------

