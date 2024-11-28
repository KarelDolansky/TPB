from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.master("local").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("./fakefriends-header.csv")

people.createOrReplaceTempView("people")

result = spark.sql("""
    SELECT age, ROUND(AVG(friends), 2) AS friends_avg
    FROM people
    GROUP BY age
    ORDER BY age
""")

result.show()

spark.stop()