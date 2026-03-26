from pyspark import SparkContext, SparkConf
import re

conf = SparkConf().setAppName("WordCountPro").setMaster("local[*]")
sc = SparkContext(conf=conf)

text = sc.textFile("file:///home/prime/spark_lab/warandsociety.txt")

# Регулярное выражение для поиска только слов (букв)
# lower() - к нижнему регистру, re.findall - только слова
words = text.flatMap(lambda line: re.findall(r'[a-zA-Zа-яА-Я]+', line.lower()))             .map(lambda word: (word, 1))             .reduceByKey(lambda a, b: a + b)

for word, count in words.sortBy(lambda x: x[1], ascending=False).take(10):
    print(f"{word}: {count}")

sc.stop()
