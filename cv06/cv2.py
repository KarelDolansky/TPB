from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

# Inicializace SparkSession
spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()

# Načtení dat ze souborů CSV
file_paths = ["1987.csv", "1988.csv", "1989.csv"]  # Cesty k datům
dfs = [spark.read.option("header", "true").csv(fp, inferSchema=True) for fp in file_paths]
data = dfs[0]
for df in dfs[1:]:
    data = data.union(df)

# Filtrace zrušených letů a odstranění chybějících hodnot
data = data.filter((col("Cancelled") == 0) & (col("ArrDelay").isNotNull()))

# Příprava cílové proměnné (zpoždění jako binární label)
data = data.withColumn("label", when(col("ArrDelay") > 0, 1).otherwise(0))

# Převod textových sloupců na číselné pomocí StringIndexer
origin_indexer = StringIndexer(inputCol="Origin", outputCol="OriginIndexed")
dest_indexer = StringIndexer(inputCol="Dest", outputCol="DestIndexed")

# Přetypování sloupce Distance na číselný typ
data = data.withColumn("Distance", col("Distance").cast("double"))

# Výběr relevantních příznaků
features = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'CRSDepTime', 'CRSArrTime', 
            'CRSElapsedTime', 'OriginIndexed', 'DestIndexed', 'Distance']
assembler = VectorAssembler(inputCols=features, outputCol="features")

# Model logistické regrese
lr = LogisticRegression(featuresCol="features", labelCol="label")

# Pipeline
pipeline = Pipeline(stages=[origin_indexer, dest_indexer, assembler, lr])

# Rozdělení dat na trénovací a testovací sady
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Trénování modelu
model = pipeline.fit(train_data)

# Predikce na testovacích datech
predictions = model.transform(test_data)

# Vyhodnocení modelu
predictions.select("label", "prediction", "probability").show()

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
roc_auc = evaluator.evaluate(predictions)
print(f"ROC AUC: {roc_auc}")
