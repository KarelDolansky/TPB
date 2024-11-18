from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Inicializace SparkSession
spark = SparkSession.builder \
    .appName("RealEstatePrediction") \
    .getOrCreate()

# Načtení dat
data_path = "realestate.csv"  # Změň na cestu k souboru
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Výběr relevantních sloupců
selected_columns = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "PriceOfUnitArea"]
data = data.select(*selected_columns)

# Příprava dat pomocí VectorAssembler
assembler = VectorAssembler(
    inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"],
    outputCol="features"
)
data = assembler.transform(data).select("features", "PriceOfUnitArea")

# Rozdělení dat na tréninkovou a testovací sadu
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Vytvoření a trénování modelu
dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")
model = dt.fit(train_data)

# Predikce na testovací sadě
predictions = model.transform(test_data)

# Vyhodnocení modelu
evaluator = RegressionEvaluator(
    labelCol="PriceOfUnitArea",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
for prediction in predictions.collect():
    print(f"{prediction.prediction:.2f}, {prediction.PriceOfUnitArea:.2f}")

print(f"Root Mean Squared Error (RMSE): {rmse}")

# Ukončení SparkSession
spark.stop()
