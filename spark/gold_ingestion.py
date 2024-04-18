#!/usr/bin/env python
# coding: utf-8

import sys
import re
from datetime import date
from ast import literal_eval
import boto3

from pyspark.sql import SparkSession
from pyspark.sql import functions as SF

YEAR_PATTERN = r"([\d]{4})-"
MONTH_PATTERN = r"-([\d]{2})"

def get_secret(secret_name:str= "grupob/rds/credentials"):

    region_name = "us-east-1"

    # Create a Secrets Manager client
    
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    
    return secret

def init_spark(app_name:str="SPARK APPLICATION"):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

def load_data(spark, input_table, year_month_filter:str=None):
    print("LOAD DATE")
    df = spark.sql(f"SELECT * FROM {input_table}")
    
    if year_month_filter and year_month_filter.upper()!= "NONE":
        year_re = re.findall(YEAR_PATTERN, year_month_filter)
        if year_re:
            df = df.filter(f"year= '{year_re[0]}'")
        
        month_re = re.findall(MONTH_PATTERN, year_month_filter)
        if month_re:
            df = df.filter(f"month = '{month_re[0]}'")
        
    print(f"COUNT:{df.count()}")
        
    return df

def transform_data(df):
    print("TRANSFORM DATA")
     
    df = df.withColumn("company", 
                            SF.when(SF.col("hvfhs_license_num") == "HV0002", SF.lit("Juno"))
                            .when(SF.col("hvfhs_license_num") == "HV0003", SF.lit("Uber"))
                            .when(SF.col("hvfhs_license_num") == "HV0004", SF.lit("Via"))
                            .when(SF.col("hvfhs_license_num") == "HV0005", SF.lit("Lyft"))
                            .otherwise("NA")
                        ).drop("hvfhs_license_num")
    
    df = df.withColumn('wait_time', SF.round(
                                                (SF.unix_timestamp("pickup_datetime") - SF.unix_timestamp('request_datetime'))/60,2))
    
    df = df.withColumn("hour", SF.hour("request_datetime"))\
            .withColumn("day_of_week", SF.dayofweek("request_datetime"))\
            .withColumn("trip_time_minutes", SF.round(SF.col("trip_time")/60,2))\
            .withColumn("date", SF.col("request_datetime").cast("date"))
    
    df = df.withColumnRenamed("PULocationID", "pickup_location_id")\
            .withColumnRenamed("DOLocationID", "dropoff_location_id")
    
    df = df.groupBy("company", "year", "month", "date", "hour", "pickup_location_id",
                 "dropoff_location_id")\
                .agg(SF.round(SF.sum("trip_time_minutes"),2).alias("sum_trip_time_minutes"),
                     SF.round(SF.sum("wait_time"),2).alias("sum_wait_time"),
                     SF.round(SF.sum("base_passenger_fare"),2).alias("sum_base_passenger_fare"),
                     SF.count("*").alias("register_count"))\
                .withColumn("day_of_week", SF.dayofweek("date"))
    
    df = df.select("year","month","date","day_of_week","hour","company",
                    "pickup_location_id","dropoff_location_id",
                    "sum_trip_time_minutes", "sum_wait_time",
                    "sum_base_passenger_fare", "register_count"
                   )
    
    
    return df

def save_data(spark, df, table_name:str, mode:str="append", save_to_db:bool=False, secret_name:str=None):
    print("SAVE DATA")
    df.repartition("year","month").write.mode(mode).partitionBy("year","month")\
        .format('parquet').saveAsTable(table_name)
    
    if literal_eval(save_to_db):
        credentials = get_secret(secret_name)

        df = spark.sql(f"select * from {table_name}")

        df.write.format("jdbc")\
        .option("driver","com.mysql.cj.jdbc.Driver") \
        .option("url", f"jdbc:mysql://{credentials.get('host')}:3306/gold?user={credentials.get('username')}&password={credentials.get('password')}") \
        .option("dbtable", table_name) \
        .mode(mode)\
        .save()

def main(input_table:str="silver.fhvhv_trip_data" ,
         output_table:str="gold.fhvhv_trip_data",
         year_month_filter:str=None,
         mode:str="append",
         save_to_db:bool=False, 
         secret_name:str="grupob/rds/credentials"):
    print("GOLD INGESTION")
    
    spark = init_spark("GOLD_INGESTION")
    
    df = load_data(spark,input_table, year_month_filter)
    df = transform_data(df)
    save_data(spark, df, output_table, mode, save_to_db, secret_name)

if __name__ == "__main__":
    print("MAIN")
    #to test
    """
    sys.argv.insert(1, "default.fhvhv_trip_data_silver")
    sys.argv.insert(2, "default.fhvhv_trip_data_gold")
    sys.argv.insert(3, "None")
    sys.argv.insert(4, "2020-05")
    sys.argv.insert(5, "overwrite")
    sys.argv.insert(6, "True")
    """

    print(f"INPUTS: {sys.argv}")
    
    input_table = sys.argv[1]
    output_table = sys.argv[2]
    year_month_filter = sys.argv[3]
    write_mode = sys.argv[4]
    save_to_db = sys.argv[5]
    secret_name = sys.argv[6]

    main(input_table, output_table, year_month_filter, write_mode, save_to_db, secret_name)
    