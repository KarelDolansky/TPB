from pyspark import SparkConf, SparkContext
import re
import json

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.findall(r'\b\w+\b', text.lower())

input = sc.textFile("./book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sortedWordCounts = wordCounts.sortBy(lambda x: x[1], ascending=False)
top20Words = sortedWordCounts.take(20)

for word, count in top20Words:
    print(f"{word} {count}")

# Bonus
with open('./small.jsonl', 'r', encoding='utf-8') as file:
    articles = [json.loads(line) for line in file]

words = sc.parallelize([article['content'] for article in articles])
filteredWords = words.flatMap(normalizeWords).filter(lambda x: len(x) >= 6)
wordCounts = filteredWords.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sortedWordCounts = wordCounts.sortBy(lambda x: x[1], ascending=False)
top20Words = sortedWordCounts.take(20)

for word, count in top20Words:
    print(f"{word} {count}")
