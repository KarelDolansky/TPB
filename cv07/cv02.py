from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, regexp_replace, lower

spark = SparkSession.builder.appName("FileWordCount").getOrCreate()

input_dir = "./data"

lines = (
    spark.readStream.format("text")
    .option("path", input_dir)
    .load()
)

words = lines.select(explode(split(col("value"), "\\s+")).alias("word"))

cleaned_words = (
    words.select(
        lower(regexp_replace(col("word"), "[^a-zA-Z0-9]", "")).alias("word")
    )
    .filter("word != ''")
)

word_counts = (
    cleaned_words.groupBy("word")
    .count()
    .orderBy(col("count").desc())
)

query = (
    word_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
