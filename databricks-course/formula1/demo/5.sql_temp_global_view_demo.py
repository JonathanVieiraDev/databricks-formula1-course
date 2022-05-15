# Databricks notebook source
# MAGIC %md
# MAGIC #### Global temporary Views
# MAGIC 
# MAGIC ##### Objectives
# MAGIC 
# MAGIC * Create Global Temporary views on dataframes
# MAGIC * Access the view from SQL cell
# MAGIC * Access the view from Python cell
# MAGIC * Access the view from another notebook

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) 
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------


