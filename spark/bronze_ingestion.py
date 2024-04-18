#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import re
from datetime import date


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as SF


# In[3]:


YEAR_MONTH_PATTERN = r"([\d]{4}-\d{2}).parquet"


# In[4]:


def init_spark(app_name:str="SPARK APPLICATION"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()


# In[5]:


def load_data(spark, file_name):
    print("LOAD DATE")
    return spark.read.parquet(file_name)    


# In[6]:


def transform_data(df):
    print("TRANSFORM DATA")
    
    today = date.today()
    print(f"TODAY: {today}")
    
    df = df.\
            withColumn("file_path", SF.input_file_name()).\
            withColumn("year_month", SF.regexp_extract("file_path", YEAR_MONTH_PATTERN, 1)).\
            withColumn("load_date", SF.lit(today)).\
            withColumn("wav_match_flag",SF.col("wav_match_flag").cast("string")).\
            drop("file_path")
    
    columns_types = df.dtypes
    for col, dtype in columns_types:
        if dtype == "timestamp_ntz":
            print(f"CASTING {col} to TIMESTAMP")
            df = df.withColumn(col, SF.col(col).cast("timestamp"))

            
    return df


# In[7]:


def save_data(df, table_name:str, mode:str="overwrite"):
    print("SAVE DATA")
    df.write.mode(mode).partitionBy("load_date").format('parquet').saveAsTable(table_name)


# In[8]:


def main(file_name:str, table_name:str="bronze.fhvhv_trip_data", mode:str="append"):
    print("BRONZE INGESTION")
    print(f"FILE: {file_name}")
    
    spark = init_spark("BRONZE_INGESTION")
    
    df = load_data(spark, file_name)
    df = transform_data(df)
    save_data(df, table_name, mode)


# In[9]:


if __name__ == "__main__":
    print("MAIN")
    #to test
    #sys.argv.insert(1, "../data/fhvhv_tripdata_2020-05.parquet")
    #sys.argv.insert(2, "bronze.fhvhv_trip_data")
    #sys.argv.insert(3, "append")
    print(f"INPUTS: {sys.argv}")
    
    file_name = sys.argv[1]
    table_name = sys.argv[2]
    write_mode = sys.argv[3]
    
    main(file_name, table_name, write_mode)
    


# In[ ]:




