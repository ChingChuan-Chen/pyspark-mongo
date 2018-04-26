import pyspark
spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
print(spark.range(10).collect())
