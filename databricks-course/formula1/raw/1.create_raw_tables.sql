-- Databricks notebook source
CREATE DATABASE f1_raw;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
circuitId STRING, 
circuitRef STRING, 
name STRING, 
location STRING, 
country STRING, 
lat DOUBLE, 
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path '/mnt/storagedatabrickscourse/raw/circuits.csv', header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create races table 

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
      year INT,
      round INT,
      circuitId INT,
      name STRING,
      date DATE,
      time STRING,
      url STRING)
USING csv
OPTIONS (path "/mnt/storagedatabrickscourse/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create constructors table 
-- MAGIC * Single Line JSON 
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "/mnt/storagedatabrickscourse/raw/constructors.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create drivers table 
-- MAGIC * Single Line JSON 
-- MAGIC * Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/storagedatabrickscourse/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create results table 
-- MAGIC * Single Line JSON 
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId int,
  constructorId int,
  number int, grid int,
  position int,
  positionText STRING,
  positionOrder STRING,
  points int,
  laps int,
  time string,
  milliseconds int,
  fastestLap int,
  rank int,
  fastestLapTime STRING,
  fastestLapSpeed float,
  statusId STRING
)
USING json
OPTIONS (path "/mnt/storagedatabrickscourse/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create pit stops table 
-- MAGIC * Multi Line JSON 
-- MAGIC * Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  driverId int,
  duration string,
  lap int,
  milliseconds int,
  raceId int,
  stop int,
  time STRING
)
USING json
OPTIONS (path "/mnt/storagedatabrickscourse/raw/pit_stops.json", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Create tables for list of files

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create Lap Times Table 
-- MAGIC * CSV file
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId int,
  driverId int,
  lap int,
  position int,
  time STRING,
  milliseconds int
)
USING csv
OPTIONS (path "/mnt/storagedatabrickscourse/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create Qualifying Table
-- MAGIC * JSON file
-- MAGIC * MultiLine JSON 
-- MAGIC * Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  constructorId int,
  driverId int,
  number int,
  position int,
  q1 STRING,
  q2 STRING,
  q3 STRING,
  qualifyId INT,
  raceId int
)
USING json
OPTIONS (path "/mnt/storagedatabrickscourse/raw/qualifying", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;

-- COMMAND ----------


