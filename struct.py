import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import struct, col, collect_list

df_pd = pd.read_csv("profile.csv")

spark = SparkSession\
        .builder\
        .appName("dataTransform")\
        .getOrCreate()
schema = StructType(
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("sex", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("pairId", StringType(), True),
    StructField("statusCheck", StringType(), True),
    StructField("memo", StringType(), True)
)
df = spark.createDataFrame(df_pd, schema)

grpByCols = ["id", "name", "sex", "age"]
historyCols = ["pairId", "statusCheck", "memo"]
outDf = df.withColumn("history", struct(*[col(name) for name in historyCols]))\
    .drop(*historyCols).groupBy(*grpByCols)\
    .agg(collect_list(col("history")).alias("history"))
outDf.show()

