from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Vytvoření SparkSession
spark = SparkSession.builder.master("spark://fa367db42f31:7077").appName("SparkSQL").getOrCreate()

# Načtení CSV souboru s hlavičkou a inferencí schématu
people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")

# Výpis schématu
print("Here is our inferred schema:")
people.printSchema()

# Výpočet průměrného počtu přátel podle věku
average_friends_by_age = people.groupBy("age").agg(avg("friends").alias("friends_avg")).orderBy("age")

# Výpis výsledků
average_friends_by_age.show()

# Ukončení SparkSession
spark.stop()
