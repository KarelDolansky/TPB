from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = int(fields[0])
    itemId = int(fields[1])
    price = float(fields[2])
    return (customerId, itemId, price)

lines = sc.textFile("./customer-orders.csv")
parsedLines = lines.map(parseLine)

customerSpendings = parsedLines.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: x + y)
sortedSpendings = customerSpendings.sortBy(lambda x: x[1], ascending=False)
results = sortedSpendings.collect()

for result in results:
    print(f"{result[0]} {result[1]:.2f}")