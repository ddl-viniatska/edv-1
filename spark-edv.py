import sys
import os
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("Test mounts") \
    .getOrCreate()

data_filename = "diabetes.csv"

dir_root = "/mnt"
dir_mount = "diabetes"
dir_fullpath = os.path.join(dir_root, dir_mount)
dir_fullpath_file = os.path.join(dir_fullpath, data_filename)
dir_writepath = os.path.join(dir_fullpath, os.getenv("DOMINO_RUN_ID")+"_parquet")

df_edv = spark.read.option("header", True).csv(dir_fullpath_file)

print(df_edv.show())

df_edv_new = df_edv.withColumn("GlucoseToBMI",F.col("Glucose")/F.col("BMI"))

print(df_edv_new)

df_edv_new.write.parquet(dir_writepath)
