
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

# Iniciar Apache Spark
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Tarea3_AnalisisVentas") \
    .config("spark.python.worker.reuse", "false") \
    .getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# Datos de prueba de una tienda online
ventas = [
    ("Laptop", 2, 800),
    ("Mouse", 5, 20),
    ("Teclado", 3, 50),
    ("Laptop", 1, 800),
    ("Mouse", 4, 20)
]

# Crear un DataFrame
df = spark.createDataFrame(
    ventas, ["Producto", "Cantidad", "Precio"]
)

print("\n=== DATOS DE VENTAS ===")
df.show()

# Calcular el total por venta
df = df.withColumn(
    "Total", col("Cantidad") * col("Precio")
)

print("\n=== VENTAS CON TOTAL ===")
df.show()

# Agrupar ventas por producto
resultado = df.groupBy("Producto").agg(
    spark_sum("Total").alias("Ventas_Totales")
)

print("\n=== VENTAS POR PRODUCTO ===")
resultado.show()

spark.stop()
