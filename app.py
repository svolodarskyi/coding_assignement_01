# Databricks notebook source
import os

# COMMAND ----------

df=spark.read.csv(f"file:{os.getcwd()}/data/*.csv", header=True) 

display(df)

# COMMAND ----------


