#!/usr/bin/env python
# coding: utf-8

# In[7]:


import sys
import re
from datetime import date


# In[8]:


from pyspark.sql import SparkSession
from pyspark.sql import functions as SF


# In[9]:


YEAR_PATTERN = r"([\d]{4})-"
MONTH_PATTERN = r"-(\d{2})"


# In[10]:


def init_spark(app_name:str="SPARK APPLICATION"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()


# In[37]:


def load_data(spark, input_table, load_date_filter:str=None, year_month_filter:str=None):
    print("LOAD DATE")
    df = spark.sql(f"SELECT * FROM {input_table}")
    if load_date_filter and load_date_filter.upper()!= "NONE":
        df = df.filter(f"load_date = CAST('{load_date_filter}' AS DATE)")
    if year_month_filter and year_month_filter.upper()!= "NONE":
        df = df.filter(f"year_month = '{year_month_filter}'")
        
    print(f"COUNT:{df.count()}")
        
    return df


# In[22]:


def transform_data(df):
    print("TRANSFORM DATA")
    
    
    df = df.drop(
                    "airport_fee","dispatching_base_num",
                    "originating_base_num",
                    "tips",
                    "shared_request_flag",
                    "shared_match_flag",
                    "access_a_ride_flag",
                    "wav_request_flag",
                    "wav_match_flag",
                    "driver_pay"
                    "tolls",
                    "bcf",
                    "sales_tax",
                    "congestion_surcharge"
    )
    
    df = df.distinct()
    df = df.filter("base_passenger_fare > 0.0")
    
    df = df.withColumn("year", SF.regexp_extract("year_month", YEAR_PATTERN, 1))
    df = df.withColumn("month", SF.regexp_extract("year_month", MONTH_PATTERN, 1))
    
    df = df.drop("year_month")
    
    return df


# In[23]:


def save_data(df, table_name:str, mode:str="overwrite"):
    print("SAVE DATA")
    df.write.mode(mode).partitionBy("year","month").format('parquet').saveAsTable(table_name)


# In[24]:


def main(input_table:str="bronze.fhvhv_trip_data" ,
         output_table:str="silver.fhvhv_trip_data",
         load_date_filter:str=None,
         year_month_filter:str=None,
         mode:str="append"):
    print("SILVER INGESTION")
    
    spark = init_spark("SILVER_INGESTION")
    
    df = load_data(spark,input_table, load_date_filter, year_month_filter)
    df = transform_data(df)
    save_data(df, output_table, mode)


# In[39]:


if __name__ == "__main__":
    print("MAIN")
    #to test
    #sys.argv.insert(1, "default.fhvhv_trip_data")
    #sys.argv.insert(2, "default.fhvhv_trip_data_silver")
    #sys.argv.insert(3, "2024-03-23")
    #sys.argv.insert(4, "2020-05")
    #sys.argv.insert(5, "overwrite")
    
    print(f"INPUTS: {sys.argv}")
    
    input_table = sys.argv[1]
    output_table = sys.argv[2]
    load_date_filter = sys.argv[3]
    year_month_filter = sys.argv[4]
    write_mode = sys.argv[5]
    
    main(input_table, output_table, load_date_filter, year_month_filter, write_mode)
    


# In[ ]:




