from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, window, regexp_replace, lower, current_timestamp

spark = SparkSession.builder.appName("cv1").getOrCreate()

lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

words = lines.select(explode(split(col("value"), "\\s+")).alias("word"))

cleaned_words = (
    words.select(
        lower(regexp_replace(col("word"), "[^a-zA-Z0-9]", "")).alias("word"),
        current_timestamp().alias("timestamp")
    )
    .filter("word != ''")
)

word_counts = (
    cleaned_words.groupBy(window(col("timestamp"), "30 seconds", "15 seconds"), col("word"))
    .count()
    .select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        col("word"),
        col("count")
    )
    .orderBy(col("start"), col("count").desc())
)

query = (
    word_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
