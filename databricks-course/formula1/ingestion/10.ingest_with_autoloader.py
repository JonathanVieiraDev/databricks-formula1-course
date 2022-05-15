# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[ 
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

# df = (spark.read.format('csv')
#      .schema(circuits_schema)
#      .option('header', True)
#      .load('wasbs://landing@storagedatabrickscourse.blob.core.windows.net/'))

# df.write.format('delta').save('/mnt/storagedatabrickscourse/demo/base/autostream/')

df = spark.read \
                .option('header', True) \
                .schema(circuits_schema) \
                .csv('/mnt/storagedatabrickscourse/landing/circuits.csv')

df.write.format('delta').save('/mnt/storagedatabrickscourse/demo/base/autostream/')
df.write.format('delta').saveAsTable('demo.circuits_autoloader')

# COMMAND ----------

cloudfile = { 
    "cloudFiles.subscriptionId": "",
    "cloudFiles.connectionString": queuesas,
    "cloudFiles.format": "csv",
    "cloudFiles.tenantId": dbutils.secrets.get(scope="formula1-scope", key="databricks-tenant-id"),
    "cloudFiles.clientId": dbutils.secrets.get(scope="formula1-scope", key="databricks-app-client-id"), 
    "cloudFiles.clientSecret": dbutils.secrets.get(scope="formula1-scope", key="databricks-client-secret"),
    "cloudFiles.resourceGroup": "rg-databricks-course",
    "cloudFiles.useNotifications": "true"   
}

# COMMAND ----------

filePath = '/mnt/storagedatabrickscourse/landing'

df = (spark.readStream.format('cloudFiles')
      .options(**cloudfile)
      .option('Header', True)
      .schema(circuits_schema)
      .load(filePath)
     )

# COMMAND ----------

from delta.tables import * 
def upsertToDelta(microbatchOutputDF, batchId):
    
    deltadf = DeltaTable.forName(spark, 'demo.circuits_autoloader')
    
    (deltadf.alias('t')
     .merge(
         microbatchOutputDF.alias('s'),
         's.circuitId = t.circuitId')
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
    )

# COMMAND ----------

from pyspark.sql.streaming import * 

# (df.writeStream
#     .format('delta')
#     .trigger(once=True)
#      .option('checkpointLocation', 'dbfs:/mnt/storagedatabrickscourse/demo/base/autostream_chk/')
#      .start('dbfs:/mnt/storagedatabrickscourse/demo/base/autostream')
# )

from pyspark.sql.streaming import * 

streamQuery = (df.writeStream
               .format('delta')
               .outputMode('append')
               .foreachBatch(upsertToDelta)
               .queryName('DAISMerge')
               .trigger(once=True)
               .option('checkpointLocation', 'dbfs:/mnt/storagedatabrickscourse/demo/base/autostream_chk/')
               .start()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from demo.circuits_autoloader;

# COMMAND ----------

# newdf = spark.read.format('delta').load('dbfs:/mnt/storagedatabrickscourse/demo/base/autostream/')

# newdf.count()

# COMMAND ----------

# display(newdf)

# COMMAND ----------


