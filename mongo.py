from pyspark.sql import SparkSession

uri = "mongodb://admin:password@192.168.1.112"
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", uri + "/admin.test") \
    .config("spark.mongodb.output.uri", uri + "/admin.test") \
    .getOrCreate()

people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
   ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])

people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

