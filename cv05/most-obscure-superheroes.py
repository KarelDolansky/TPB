from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Vytvoření SparkSession
spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

# Definování schématu pro načítání jmen
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

# Načtení souboru s názvy
names = spark.read.schema(schema).option("sep", " ").csv("./marvel-names.txt")

# Načtení souboru s grafem
lines = spark.read.text("./marvel-graph.txt")

# Zpracování propojení superhrdinů
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Najít minimální počet propojení (kde počet propojení je alespoň 1)
min_connections = connections.filter(func.col("connections") > 0).agg(func.min("connections")).first()[0]

# Najít všechny superhrdiny s tímto minimálním počtem propojení
leastConnected = connections.filter(func.col("connections") == min_connections)
leastConnected2 = connections.filter(func.col("connections") == 1)

# Přiřadit jména k ID
leastConnectedWithNames = leastConnected.join(names, "id").select("name", "connections")
leastConnectedWithNames2 = leastConnected2.join(names, "id").select("name", "connections")

# Výpis výsledků
leastConnectedWithNames.show()
leastConnectedWithNames2.show()

# Ukončení SparkSession
spark.stop()
