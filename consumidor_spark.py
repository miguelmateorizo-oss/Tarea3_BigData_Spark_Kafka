from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("KafkaPySparkVentas")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

esquema = (
    StructType()
    .add("producto", StringType())
    .add("cantidad", IntegerType())
    .add("precio", IntegerType())
)

ventas_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ventas")
    .option("startingOffsets", "earliest")
    .load()
)

ventas = (
    ventas_kafka
    .selectExpr("CAST(value AS STRING) AS mensaje")
    .select(from_json(col("mensaje"), esquema).alias("datos"))
    .select("datos.*")
    .withColumn("total", col("cantidad") * col("precio"))
)

consulta = (
    ventas.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoint_kafka_ventas")
    .start()
)

print("PYSPARK CONECTADO A KAFKA")
consulta.awaitTermination()

