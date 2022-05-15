# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Inner join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                    .filter('circuit_id < 70') \
                    .withColumnRenamed('name', 'circuit_name')

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Outer joins

# COMMAND ----------

# Left Outer Join 

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# Right Outer Join 

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# full Outer Join 

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi Joins

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Anti Joins

# COMMAND ----------

races_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Cross Joins

# COMMAND ----------

races_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(races_circuits_df)

# COMMAND ----------

races_circuits_df.count()

# COMMAND ----------


