
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
from pathlib import Path

# Iniciar Spark
spark = (
    SparkSession.builder
    .appName("StreamingVentasOnlineRetail")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.default.parallelism", "2")
    .getOrCreate()
)



spark.sparkContext.setLogLevel("ERROR")

carpeta = Path(__file__).resolve().parent
entrada = carpeta / "streaming_entrada"
checkpoint = carpeta / "streaming_checkpoint"

# Definir el esquema a partir de un archivo CSV
esquema = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(entrada / "lote_1.csv"))
    .schema
)

# Leer los archivos mediante Structured Streaming
ventas = (
    spark.readStream
    .option("header", True)
    .option("maxFilesPerTrigger", 1)
    .schema(esquema)
    .csv(str(entrada))
)

# Calcular ingresos por venta
ventas = ventas.withColumn(
    "TotalVenta",
    col("Quantity") * col("UnitPrice")
)

# Agrupar por pais
resultado = ventas.groupBy("Country").agg(
    spark_sum("TotalVenta").alias("Ingresos")
)

# Mostrar los resultados acumulados
consulta = (
    resultado.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", str(checkpoint))
    .trigger(processingTime="5 seconds")
    .start()
)

print("STREAMING INICIADO CORRECTAMENTE")

try:
    consulta.processAllAvailable()
finally:
    if consulta.isActive:
        consulta.stop()
    spark.streams.awaitAnyTermination(10)
    spark.stop()

print("SIMULACION FINALIZADA")

