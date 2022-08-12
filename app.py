# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Import Libraries

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, upper, lower, row_number, col, when, lit, asc, avg, round, max, expr
from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
import re
import os
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Functions

# COMMAND ----------

def create_database(spark, dbname: str, dbpath: str) -> None:
  spark.sql(f""" CREATE DATABASE IF NOT EXISTS {dbname} COMMENT 'PlayCare Daycare Attendance' LOCATION  '{dbpath}' """)
  
def load_csv_files(folder_path: str) -> DataFrame:
  folder_path = folder_path + "/" if not folder_path.endswith('/') else folder_path
  return spark.read.csv(f"{folder_path}*.csv", sep = ',', header=True) 

def get_table_name_from_path(path: str) -> str:
  return path[path.rfind('/') + 1:]
  
def register_table(spark, dbname: str, tblname: str, location: str, schema: str = None) -> None:
  schema = " " if schema is None else f"({schema})"
  spark.sql(f""" CREATE TABLE IF NOT EXISTS {dbname}.{tblname} {schema} USING DELTA LOCATION '{location}' """)
  
def load_bronze(spark, df: DataFrame, location: str) -> None:
  df.write.format("delta").mode("append").option("mergeSchema", "true").save(location)

def get_numeric_cols(df: DataFrame) -> List:
  return  [column for column in df.columns if re.search(r'\d', column)]

def remove_duplicates_by_ranking(df: DataFrame, partition_by: str, order_by: str) -> DataFrame:
  return df.withColumn("row_num", row_number().over(Window.partitionBy(partition_by).orderBy(order_by))).filter(col('row_num') == 1).drop(col("row_num"))

def unpivot(df: DataFrame, cols_keep: List, cols_unpivot: List, var_col_name: str ,value_col_name: str) -> DataFrame:
  return df.to_pandas_on_spark().melt(id_vars=[*cols_keep], value_vars=[*cols_unpivot], var_name=var_col_name, value_name=value_col_name).to_spark()

def return_records_condition(df: DataFrame, filter_col: str, filter_vals: List, return_col: str) -> List:
  df = df.filter(lower(col(filter_col)).isin([x.lower() for x in filter_vals])).select(return_col).collect()
  return [row[return_col] for row in df]

def filter_out_values(df: DataFrame, filter_col: str, filter_out_vals: List) -> DataFrame:
  return df.filter(col(filter_col).isin(*filter_out_vals)==False)

def get_last_loaded_date(spark, dbname: str, tblname: str, date_column: str) -> str:
  spark.sql(f"""SELECT MAX({date_column}) from {dbname}.{tblname}""").collect()[0]

def load_silver(spark, silver_path: str, update_df: DataFrame, last_loaded: str) -> None:
  
  target = DeltaTable.forPath(spark, bronze_path)

  target.alias('silver') \
    .merge(
      source = update_df.filter(col(silver_loaded_timestamp_utc) > last_loaded).alias('updated'),
      condition = 'silver.Date = updated.Date and silver.hours = updated.hours'
    ) \
    .whenMatchedUpdate(set = 
      {
        "hours": "bronze.hours", 
        "silver_loaded_timestamp_utc": "updated.silver_loaded_timestamp_utc"
      }
     ) \
    .whenNotMatchedInsert(values =
      {
        "Day": "updates.Day",
        "Date": "updates.Date",
        "hours": "updates.hours",
        "attendance_count": "updates.attendance_count",
        "silver_loaded_timestamp_utc": "updated.silver_loaded_timestamp_utc"
      }
    ).execute()

def load_silver_initial(spark, df: DataFrame, location: str) -> None:
  df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(location)
  
def load_gold(spark, df: DataFrame, location: str) -> None:
  df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(location)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Setup

# COMMAND ----------

src_files_path = 'dbfs:/FileStore/tables/sergii/daycare_analysis_project/'

dbpath = 'dbfs:/temp/'
dbname = 'daycare_numbers_sv'

bronze_path = f"""{dbpath}/{dbname}/attendance_bronze"""
silver_path = f"""{dbpath}/{dbname}/attendance_silver"""
gold_path_max_attendance = f"""{dbpath}/{dbname}/max_attendance_by_time"""
gold_path_avg_attendance = f"""{dbpath}/{dbname}/avg_attendance_by_day_and_time"""

silver_shema = "day STRING, date STRING, hours STRING, attendance_count INT, silver_loaded_timestamp_utc TIMESTAMP"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Source -> Bronze

# COMMAND ----------

create_database(spark, dbname, dbpath)

raw_df= load_csv_files(src_files_path)

load_bronze(
          spark = spark, 
          df = raw_df.withColumn("bronze_loaded_timestamp_utc", current_timestamp()), 
          location = bronze_path
       ) 

register_table(
        spark = spark, 
        dbname = dbname, 
        tblname = get_table_name_from_path(bronze_path), 
        location = bronze_path
      )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Bronze -> Silver

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)

df_bronze_clean = (
              df_bronze
              .drop('Total', 'Column1', 'Total2')
              .withColumn('Day', upper('Day'))
              .withColumnRenamed('8.30-9.00', '0830-9000')
              .withColumnRenamed('9.00-10.00', '0900-1000')
              .withColumnRenamed('10.00-11.00', '1000-1100')
              .withColumnRenamed('11.00-12.00', '1100-1200')
              .withColumnRenamed('12.00-1.00', '1200-1300')
              .withColumnRenamed('5.00-6.00', '1700-1800')
              .withColumnRenamed('6.00-7.00', '1800-1900')
              .withColumnRenamed('7.00-8.00', '1900-2000')
            )

df_bronze_de_duplicate = remove_duplicates_by_ranking(
              df = df_bronze_clean, 
              partition_by='Date', 
              order_by='InsertDateTime'
      )

df_bronze_unpivot_hours = unpivot(
          df = df_bronze_de_duplicate, 
          cols_keep =['Day', 'Date'], 
          cols_unpivot = get_numeric_cols(df_bronze_de_duplicate), 
          var_col_name ='hours',
          value_col_name ='attendance_count'
      )

dates_business_closed = return_records_condition(
            df = df_bronze_unpivot_hours, 
            filter_col = 'attendance_count', 
            filter_vals = ['Closed', 'holiday'],
            return_col = 'Date'
          )

df_bronze_operation_days_only = filter_out_values(
              df =df_bronze_unpivot_hours,
              filter_col = 'Date',
              filter_out_vals = dates_business_closed  
             )

df_bronze_final = (
          df_bronze_operation_days_only
          .na.fill({'attendance_count': 0})
          .withColumn("silver_loaded_timestamp_utc", current_timestamp())
          .withColumnRenamed('Day', 'day')
          .withColumnRenamed('Date', 'date')
          .withColumn("attendance_count", col('attendance_count').cast('int'))
          
      )
try:
  last_loaded_bronze = get_last_loaded_date(
          spark = spark, 
          dbname = dbname, 
          tblname = get_table_name_from_path(bronze_path)
      )  
  load_silver(spark = spark, silver_path = silver_path, update_df = df_bronze_final, last_loaded = last_loaded_silver)
except:
  load_silver_initial(
                    spark = spark, 
                    df = df_bronze_final, 
                    location = silver_path
                 )
  
register_table(
        spark = spark, 
        dbname = dbname, 
        tblname = get_table_name_from_path(silver_path), 
        location = silver_path, 
        schema = silver_shema
      )  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Silver -> Gold

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)

# avg_attendance_by_day_and_time
avg_att_df = df_silver.groupBy('day').pivot('hours').agg(round(avg("attendance_count"),0))

load_gold(spark = spark, df = avg_att_df, location = gold_path_max_attendance)

register_table(
               spark = spark, 
               dbname = dbname, 
               tblname = get_table_name_from_path(gold_path_max_attendance), 
               location = gold_path_max_attendance, 
               schema = None
          )

#max_attendance_by_time
max_att_df = (
       df_silver.groupBy('hours').pivot('hours').max("attendance_count")
      .select(
          expr('max(`0830-9000`)').alias('0830-9000'), 
          expr('max(`0900-1000`)').alias('0900-1000'),
          expr('max(`1000-1100`)').alias('1000-1100'), 
          expr('max(`1100-1200`)').alias('1100-1200'), 
          expr('max(`1200-1300`)').alias('1200-1300'), 
          expr('max(`1700-1800`)').alias('1700-1800'), 
          expr('max(`1800-1900`)').alias('1800-1900'), 
          expr('max(`1900-2000`)').alias('1900-2000')
      )
    )

load_gold(spark = spark, df = max_att_df, location = gold_path_avg_attendance)

register_table(
               spark = spark, 
               dbname = dbname, 
               tblname = get_table_name_from_path(gold_path_avg_attendance), 
               location = gold_path_avg_attendance, 
               schema = None
          )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Analyze Gold Tables

# COMMAND ----------

# MAGIC %sql SELECT * FROM daycare_numbers_sv.avg_attendance_by_day_and_time;

# COMMAND ----------

# MAGIC %sql SELECT * FROM daycare_numbers_sv.max_attendance_by_time

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Cleanup Resources

# COMMAND ----------

# Drop Databse

# spark.sql(f"""DROP DATABASE {dbname} CASCADE;""")

# COMMAND ----------

# Remove Source Files

# dbutils.fs.rm('dbfs:/FileStore/tables/sergii/daycare_analysis_project/', True)

# COMMAND ----------


