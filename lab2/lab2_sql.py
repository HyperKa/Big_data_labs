from pyspark.sql import SparkSession
import xml.etree.ElementTree as ET

spark = SparkSession.builder.appName("StackOverflowSQL").getOrCreate()

# загрузка таблиц
languages_df = spark.read.csv("file:///home/prime/spark_lab/lab2/programming-languages.csv", header=True)
raw_posts = spark.read.text("file:///home/prime/spark_lab/lab2/posts_sample.xml")

# парсинг XML
def parse_xml(row):
    try:
        root = ET.fromstring(row[0])
        return (root.attrib.get('CreationDate'), root.attrib.get('Tags'))
    except:
        return None

# под x[1] тут понимается тег, есть ли он вообще в записи xml (после парсинга - кортеж (data, tag)
# идет создание структуры, первый элемент кортежа мапится в date, второй в tags: <java><python> - лежит строка
posts_df = raw_posts.rdd.map(parse_xml).filter(lambda x: x is not None and x[1] is not None).toDF(["date", "tags"])

# регистрация таблицы
# ссылки на датафреймы при SELECT ... FROM из таблиц languages_table или posts_table
languages_df.createOrReplaceTempView("languages_table")
posts_df.createOrReplaceTempView("posts_table")

# SQL запрос:
# работа регулярки:
# строка "<java><python>" -> "java><python" -> split(..., '><') -> ["java","python"]
query = """
WITH ExplodedTags AS (
    SELECT 
        substring(date, 1, 4) as year,
        explode(split(regexp_extract(tags, '<(.*)>', 1), '><')) as raw_tag
    FROM posts_table
),
CleanedData AS (
    SELECT 
        year,
        lower(raw_tag) as tag
    FROM ExplodedTags
),
FilteredCounts AS (
    SELECT 
        year, 
        tag, 
        count(*) as count
    FROM CleanedData
    WHERE tag IN (SELECT lower(name) FROM languages_table)
    GROUP BY year, tag
),
RankedLangs AS (
    SELECT 
        year, 
        tag, 
        count,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY count DESC) as rank
    FROM FilteredCounts
)
SELECT year, tag, count
FROM RankedLangs
WHERE rank <= 10
ORDER BY year DESC, count DESC
"""

report = spark.sql(query)

# вывод и сохранение
report.show(100, truncate=False)
report.write.mode("overwrite").parquet("file:///home/prime/spark_lab/lab2/report.parquet")

spark.stop()
