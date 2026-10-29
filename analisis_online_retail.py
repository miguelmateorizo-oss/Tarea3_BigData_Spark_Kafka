
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc

carpeta = Path(__file__).resolve().parent

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("Tarea3_OnlineRetail")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Cargar el conjunto de datos real
df = spark.read.csv(
    str(carpeta / "online_retail.csv"),
    header=True,
    inferSchema=True
)

print("\n=== TOTAL DE REGISTROS ===")
print(df.count())

print("\n=== ESTRUCTURA DE LOS DATOS ===")
df.printSchema()

print("\n=== PRIMEROS 5 REGISTROS ===")
df.show(5, truncate=False)

# Filtrar cantidades y precios validos
ventas = df.filter(
    (col("Quantity") > 0) &
    (col("UnitPrice") > 0)
)

# Calcular valor de cada venta
ventas = ventas.withColumn(
    "TotalVenta",
    col("Quantity") * col("UnitPrice")
)

print("\n=== TOTAL DE VENTAS VALIDAS ===")
print(ventas.count())

# Calcular ventas por pais
ventas_pais = ventas.groupBy("Country").agg(
    spark_sum("TotalVenta").alias("Ingresos")
)

print("\n=== TOP 10 PAISES POR INGRESOS ===")
ventas_pais.orderBy(desc("Ingresos")).show(10)

# Analizar los 10 productos con mayores ingresos
productos_top = ventas.groupBy(
    "StockCode", "Description"
).agg(
    spark_sum("TotalVenta").alias("Ingresos")
)

print("\n=== TOP 10 PRODUCTOS POR INGRESOS ===")

productos_top.orderBy(
    desc("Ingresos")
).show(10, truncate=False)

spark.stop()
