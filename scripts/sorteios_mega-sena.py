from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema_mega_sena = StructType([
	StructField("Concurso",  IntegerType(),  True),
	StructField("Data",  StringType(),  True),
	StructField("bola 1",  IntegerType(),  True),
	StructField("bola 2",  IntegerType(),  True),
	StructField("bola 3",  IntegerType(),  True),
	StructField("bola 4",  IntegerType(),  True),
	StructField("bola 5",  IntegerType(),  True),
	StructField("bola 6",  IntegerType(),  True),
])

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Mega Sena"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_mega_sena)
		          .load("/home/spark/mega-sena/data/sorteios_mega-sena.csv"))

print(df.printSchema)
df.show()
