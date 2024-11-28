from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum as _sum, desc

# Inicializace SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TotalSpentByCustomer") \
    .getOrCreate()

# Definice názvů sloupců
columns = ["customer_id", "order_id", "price"]

# Načtení dat ze souboru CSV a přiřazení názvů sloupců
data = spark.read \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .csv("./customer-orders.csv") \
    .toDF(*columns)

# Výpočet celkových výdajů každého zákazníka
total_spent = data.groupBy("customer_id") \
    .agg(round(_sum("price"), 2).alias("total_spent"))

# Seřazení výsledku podle výdajů v sestupném pořadí
total_spent.orderBy(desc("total_spent")).show()

# Ukončení SparkSession
spark.stop()
