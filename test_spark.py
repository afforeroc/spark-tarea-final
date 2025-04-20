from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PruebaGuardado").getOrCreate()

# Crear un DataFrame
data = [("Juan", 28), ("Ana", 34), ("Luis", 22)]
columns = ["nombre", "edad"]
df = spark.createDataFrame(data, columns)

# Mostrar el DataFrame
df.show()

# Guardar el DataFrame en formato CSV
df.write.mode("overwrite").option("header", "true").csv("datos_prueba_csv")

# Verificar si el DataFrame se guardó correctamente (leer el CSV de nuevo)
df2 = spark.read.option("header", "true").csv("datos_prueba_csv")

# Mostrar el contenido del DataFrame leído desde el CSV
df2.show()

# Detener la sesión de Spark
spark.stop()