from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("RealEstatePrediction") \
    .getOrCreate()

data_path = "realestate.csv"
data = spark.read.csv(data_path, header=True, inferSchema=True)

selected_columns = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "PriceOfUnitArea"]
data = data.select(*selected_columns)

assembler = VectorAssembler(
    inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"],
    outputCol="features"
)
data = assembler.transform(data).select("features", "PriceOfUnitArea")

train_data, test_data = data.randomSplit([0.9, 0.1], seed=42)

dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")
model = dt.fit(train_data)

predictions = model.transform(test_data)

evaluator = RegressionEvaluator(
    labelCol="PriceOfUnitArea",
    predictionCol="prediction",
    metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
for prediction in predictions.collect():
    print(f"{prediction.prediction:.2f}, {prediction.PriceOfUnitArea:.2f}")

print(f"Root Mean Squared Error (RMSE): {rmse}")

spark.stop()
