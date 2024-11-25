from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


spark = (
    SparkSession.builder.appName("FlightDelayPrediction")
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .config("spark.memory.fraction", "0.8")
    .getOrCreate()
)


arr = ["Year","Month","DayofMonth","DayOfWeek","CRSDepTime","CRSArrTime","UniqueCarrier_index",
       "CRSElapsedTime","Origin_index","Dest_index","Distance",
]



spark = SparkSession.builder.appName("FlightDelayPrediction").getOrCreate()

file_paths = ["1987.csv", "1988.csv", "1989.csv"]
datas = [spark.read.option("header", "true").csv(fp, inferSchema=True) for fp in file_paths]
data = datas[0]
for data in datas[1:]:
    data = data.union(data)

data = data.filter((col("Cancelled") == 0) & (col("ArrDelay").isNotNull()))

data = data.withColumn("label", when(col("ArrDelay") > 0, 1).otherwise(0))

data = data.withColumn("CRSElapsedTime", col("CRSElapsedTime").cast(IntegerType()))

data = data.withColumn("Distance", col("Distance").cast(DoubleType()))

stages = []
stages.append(StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrier_index", handleInvalid="skip").fit(data))
stages.append(StringIndexer(inputCol="Origin", outputCol="Origin_index", handleInvalid="skip").fit(data))
stages.append(StringIndexer(inputCol="Dest", outputCol="Dest_index", handleInvalid="skip").fit(data))



pipeline = Pipeline(stages=stages)

data = pipeline.fit(data).transform(data)

data = data.repartition(100)

assembler = VectorAssembler(
    inputCols=arr,
    outputCol="features",
    handleInvalid="skip",
)

data = assembler.transform(data)
train_data, test_data = data.randomSplit([0.9, 0.1], seed=42)

logistic_regression = LogisticRegression(featuresCol="features", labelCol="label")
logistic_regression_model = logistic_regression.fit(train_data)
predictions = logistic_regression_model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)
print(f"Logistic Regression Accuracy: {accuracy}")

if accuracy < 1:
    gbt_classifier = MultilayerPerceptronClassifier(featuresCol="features", labelCol="label", maxIter=50,seed=42, layers=[11, 5, 4, 2])
    gbt_classifier_model = gbt_classifier.fit(train_data)
    predictions = gbt_classifier_model.transform(test_data)
    accuracy = evaluator.evaluate(predictions)
    print(f"Random Forest Accuracy: {accuracy}")

spark.stop()