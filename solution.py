from pyspark import SparkContext, SparkConf
import math

conf = SparkConf().setAppName("BikeLab").setMaster("local[*]")
sc = SparkContext(conf=conf)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

print("\n" + "="*50)
print("НАЧАЛО ОБРАБОТКИ ДАННЫХ")
print("="*50 + "\n")

trips = sc.textFile("file:///home/prime/spark_lab/trips.csv")
t_header = trips.first()
trips_data = trips.filter(lambda x: x != t_header).map(lambda x: x.split(","))

max_bike = trips_data.map(lambda x: (x[8], int(x[1]))) \
                     .reduceByKey(lambda a, b: a + b) \
                     .sortBy(lambda x: x[1], ascending=False).first()
print(f"1. Велосипед с макс. пробегом: {max_bike[0]} (Время: {max_bike[1]} сек)")

stations = sc.textFile("file:///home/prime/spark_lab/stations.csv")
s_header = stations.first()
coords = stations.filter(lambda x: x != s_header) \
                 .map(lambda x: x.split(",")) \
                 .map(lambda x: (float(x[2]), float(x[3])))

max_dist = coords.cartesian(coords) \
                 .map(lambda x: haversine(x[0][0], x[0][1], x[1][0], x[1][1])) \
                 .max()
print(f"2. Максимальное геодезическое расстояние: {max_dist:.2f} км")

bikes_count = trips_data.map(lambda x: x[8]).distinct().count()
print(f"4. Всего велосипедов в системе: {bikes_count}")

long_trips = trips_data.filter(lambda x: int(x[1]) > 10800).count()
print(f"5. Количество длинных поездок (>3ч): {long_trips}")

print("\n" + "="*50)
print("ОБРАБОТКА ЗАВЕРШЕНА")
print("="*50 + "\n")

sc.stop()
