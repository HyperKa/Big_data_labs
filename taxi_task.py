from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("TaxiTask").setMaster("local[*]")
sc = SparkContext(conf=conf)

taxi = sc.textFile("file:///home/prime/spark_lab/nyctaxi.csv")
header = taxi.first()

taxi_counts = taxi.filter(lambda line: line != header) \
                  .map(lambda line: line.split(",")) \
                  .map(lambda row: (row[0], 1)) \
                  .reduceByKey(lambda a, b: a + b)

top10 = taxi_counts.sortBy(lambda x: x[1], ascending=False).take(10)

print("\n" + "!"*30)
print("ТОП-10 НОМЕРОВ ТАКСИ:")
for medallion, count in top10:
    print(f"Номер: {medallion}, Поездок: {count}")
print("!"*30 + "\n")

sc.stop()
