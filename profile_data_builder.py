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
    [StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("pairId", StringType(), True),
    StructField("statusCheck", StringType(), True),
    StructField("memo", StringType(), True)]
)
df = spark.createDataFrame(df_pd, schema)

grpByCols = ["id", "name", "sex", "age"]
historyCols = ["pairId", "statusCheck", "memo"]
outDf = df.withColumn("history", struct(*[col(name) for name in historyCols]))\
    .drop(*historyCols).groupBy(*grpByCols)\
    .agg(collect_list(col("history")).alias("history"))
uri = 'mongodb://admin:password@192.168.1.112'
outDf.write.format("com.mongodb.spark.sql.DefaultSource")\
    .option('uri', uri).option('database', 'admin').option('collection', 'profile')\
    .mode("append").save()
