# Databricks notebook source
storage_account = 'chandrastorageforlake'
storage_container = 'outputs'
mtcars_storage_filepath = 'output-from-blob/mtcars.parquet'
storage_account_access_key='dz8PVeGjXEPqqZchHnF1u1GEC2qawpygtuL3R6gIluA+aqRhkIUOJkzzOogMMT8LT1UrCw0SMFSV+AStcE6Tmg=='

# COMMAND ----------

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",f"{storage_account_access_key}"
    )

# COMMAND ----------

mtcars_storage_path = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/{mtcars_storage_filepath}"

# COMMAND ----------

print(mtcars_storage_path)

# COMMAND ----------

spark.read.format('parquet').load(mtcars_storage_path)

# COMMAND ----------

mtcarsdf = spark.read.format("parquet").load(mtcars_storage_path)

# COMMAND ----------

display(mtcarsdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists mtcars_tbl () 

# COMMAND ----------

COPY INTO <catalog-name>.<schema-name>.<table-name>
FROM 'abfss://<container>@<storage-account>.dfs.core.windows.net/<folder>'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'header' = 'true',
  'inferSchema' = 'true'
);

SELECT * FROM <catalog-name>.<schema-name>.<table-name>;

# COMMAND ----------

COPY INTO target_table [ BY POSITION | ( col_name [ , <col_name> ... ] ) ]
  FROM { source_clause |
         ( SELECT expression_list FROM source_clause ) }
  FILEFORMAT = data_source
  [ VALIDATE [ ALL | num_rows ROWS ] ]
  [ FILES = ( file_name [, ...] ) | PATTERN = glob_pattern ]
  [ FORMAT_OPTIONS ( { data_source_reader_option = value } [, ...] ) ]
  [ COPY_OPTIONS ( { copy_option = value } [, ...] ) ]

source_clause
  source [ WITH ( [ CREDENTIAL { credential_name |
                                 (temporary_credential_options) } ]
                  [ ENCRYPTION (encryption_options) ] ) ]