from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("BikePath").setMaster("local[*]")
sc = SparkContext(conf=conf)

trips = sc.textFile("file:///home/prime/spark_lab/trips.csv")
header = trips.first()
trips_data = trips.filter(lambda x: x != header).map(lambda x: x.split(","))

max_bike_id = trips_data.map(lambda x: (x[8], int(x[1]))) \
                         .reduceByKey(lambda a, b: a + b) \
                         .sortBy(lambda x: x[1], ascending=False) \
                         .first()[0]

print(f"\nВелосипед-чемпион: {max_bike_id}")

bike_path = trips_data.filter(lambda x: x[8] == max_bike_id) \
                       .sortBy(lambda x: x[2]) \
                       .map(lambda x: (x[3], x[6])) \
                       .collect()

print("="*30)
print(f"ПУТЬ ВЕЛОСИПЕДА {max_bike_id}:")
for i, (start, end) in enumerate(bike_path, 1):
    print(f"Поездка {i}: {start} -> {end}")
print("="*30 + "\n")

sc.stop()
