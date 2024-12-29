# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA LOADING

# COMMAND ----------

# Define storage account and SAS token details
storage_account_name = "neudamgdatalake"
sas_token = "sp=racwdlmeop&st=2024-12-29T01:07:16Z&se=2025-01-11T09:07:16Z&spr=https&sv=2022-11-02&sr=c&sig=MIUiaL09z%2BXIBd0q8o1PuZyML5KA3x5%2BIc4wFTv7zT8%3D"
container_name = "bronze"

# Unmount the existing mount point if it exists
try:
    dbutils.fs.unmount(f"/mnt/{container_name}")
except:
    pass

# Set up configuration
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point=f"/mnt/{container_name}",
    extra_configs={"fs.azure.sas."+container_name+"."+storage_account_name+".blob.core.windows.net": sas_token}
)

# Verify the mount
display(dbutils.fs.ls(f"/mnt/{container_name}"))

# COMMAND ----------

df_cal = spark.read.csv("/mnt/bronze/AdventureWorks_Calendar", header=True, inferSchema=True)

df_cus = spark.read.csv("/mnt/bronze/AdventureWorks_Customers", header=True, inferSchema=True)

df_procat = spark.read.csv("/mnt/bronze/AdventureWorks_Product_Categories", header=True, inferSchema=True)

df_pro = spark.read.csv("/mnt/bronze/AdventureWorks_Products", header=True, inferSchema=True)

df_ret = spark.read.csv("/mnt/bronze/AdventureWorks_Returns", header=True, inferSchema=True)

df_sales = spark.read.csv("/mnt/bronze/AdventureWorks_Sales*", header=True, inferSchema=True)

df_ter = spark.read.csv("/mnt/bronze/AdventureWorks_Territories", header=True, inferSchema=True)

df_subcat = spark.read.csv("/mnt/bronze/Product_Subcategories", header=True, inferSchema=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

# Define storage account and SAS token details
storage_account_name = "neudamgdatalake"
sas_token = "sp=racwdlmeop&st=2024-12-29T02:31:41Z&se=2025-01-11T10:31:41Z&spr=https&sv=2022-11-02&sr=c&sig=xXyL39bs0FZu2DM7HYqoLVts51kL1oz25hISQEyd7Y8%3D"
container_name = "silver"

# Unmount the existing mount point if it exists
try:
    dbutils.fs.unmount(f"/mnt/{container_name}")
except:
    pass

# Set up configuration
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point=f"/mnt/{container_name}",
    extra_configs={"fs.azure.sas."+container_name+"."+storage_account_name+".blob.core.windows.net": sas_token}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calendar

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))\
    .withColumn('Year', year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Calendar')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

df_cus = df_cus.withColumn('fullName', concat(col('Prefix'), lit(' '), col('FirstName'), lit(' '), col('LastName')))
df_cus.display()

# COMMAND ----------

df_cus = df_cus.withColumn('fullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Customers')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product_Categories

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_procat.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Product_Categories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                .withColumn('ProductName', split(col('ProductName'), ',')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Products')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Returns')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Territories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber', regexp_replace('OrderNumber', 'S', 'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total Orders')).display()
# click + below, and select Visualization

# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Sales')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Product Subcategories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
            .mode('append')\
            .option("path", '/mnt/silver/AdventureWorks_Product_Subcategories')\
            .save()

# COMMAND ----------

