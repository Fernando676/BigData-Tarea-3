
"""
Luis Fernando Ruiz
Materia Big Data
UNAD
Consumidor de streaming que procesa datos en tiempo real desde Kafka.
Adaptado al formato de columnas y fechas.
Realiza agregaciones por ventana de 1 hora para precipitación.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, avg, count, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Configuración Spark (incluye el paquete de Kafka)
spark = SparkSession.builder \
    .appName("StreamingClimaMejorado") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 2. Definir esquema de los datos (debe coincidir con las columnas del CSV)
schema = StructType([
    StructField("CodigoEstacion", StringType(), True),
    StructField("CodigoSensor", StringType(), True),
    StructField("FechaObservacion", StringType(), True),
    StructField("ValorObservado", DoubleType(), True),   # Ya viene como número del productor
    StructField("NombreEstacion", StringType(), True),
    StructField("Departamento", StringType(), True),
    StructField("Municipio", StringType(), True),
    StructField("ZonaHidrografica", StringType(), True),
    StructField("Latitud", StringType(), True),
    StructField("Longitud", StringType(), True),
    StructField("DescripcionSensor", StringType(), True),
    StructField("UnidadMedida", StringType(), True),
    StructField("Entidad", StringType(), True)
])

# 3. Leer stream desde Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "datos_clima") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsear JSON y limpiar
df_clean = df_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("FechaObservacion", 
                to_timestamp(col("FechaObservacion"), "yyyy MMM dd hh:mm:ss a")) \
    .dropna(subset=["ValorObservado", "FechaObservacion"])

# 5. Agregación por ventana de 1 hora para precipitación
#    Filtramos sensores que contengan "PRECIPITACI" (precipitación)
windowed_agg = df_clean \
    .filter(col("DescripcionSensor").contains("PRECIPITACI")) \
    .withWatermark("FechaObservacion", "10 minutes") \
    .groupBy(
        window(col("FechaObservacion"), "1 hour"),
        col("Departamento")
    ) \
    .agg(avg("ValorObservado").alias("avg_precipitacion"),
         count("ValorObservado").alias("num_lecturas"))

# 6. Salida a consola en modo update (solo muestra los cambios)
query_console = windowed_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming iniciado. Mostrando agregaciones por ventana de 1 hora en consola...")
query_console.awaitTermination()


        
