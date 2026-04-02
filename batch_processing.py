"""
Procesamiento por lotes de datos meteorológicos.
Muestra en consola:
- Datos limpios (primeras filas)
- Estadísticas diarias por sensor
- Estadísticas mensuales por departamento
- Detección de outliers por sensor
- Resumen general
- Top 10 sensores por cantidad de registros
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour,
    avg, min, max, count, stddev, when, lit, percentile_approx, sum, regexp_replace
)
from pyspark.sql.types import DoubleType
from pyspark.sql.utils import AnalysisException

# 1. Crear SparkSession
spark = SparkSession.builder \
    .appName("BatchProcessingClima") \
    .getOrCreate()

# 2. Cargar CSV con opciones para manejar comillas y encoding
file_path = "/home/vboxuser/57sv-p2fu.csv"
df_raw = spark.read.option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").csv(file_path)

# Mostrar columnas para verificar (opcional)
print("\nColumnas detectadas:")
print(df_raw.columns)

# 3. Limpieza inicial
# Renombrar columnas por si hay diferencias (usamos los nombres exactos del CSV)
# Los nombres pueden tener acentos, pero Spark los lee tal cual.
# Limpiar valores numéricos: eliminar comas y convertir a double
df_clean = df_raw.withColumn("ValorObservado_clean", 
                             regexp_replace(col("ValorObservado"), ",", "").cast(DoubleType())) \
                 .drop("ValorObservado") \
                 .withColumnRenamed("ValorObservado_clean", "ValorObservado")

# Convertir fecha: formato "2026 Apr 01 02:37:00 PM" -> usar patrón "yyyy MMM dd hh:mm:ss a"
# Spark soporta 'MMM' para abreviaturas de meses en inglés.
df_clean = df_clean.withColumn("FechaObservacion", 
                               to_timestamp(col("FechaObservacion"), "yyyy MMM dd hh:mm:ss a")) \
                   .dropna(subset=["ValorObservado", "FechaObservacion"])

# Extraer componentes de fecha
df_clean = df_clean.withColumn("anio", year("FechaObservacion")) \
                   .withColumn("mes", month("FechaObservacion")) \
                   .withColumn("dia", dayofmonth("FechaObservacion")) \
                   .withColumn("hora", hour("FechaObservacion"))

# Convertir latitud y longitud a double si existen
if "Latitud" in df_clean.columns and "Longitud" in df_clean.columns:
    df_clean = df_clean.withColumn("Latitud", col("Latitud").cast(DoubleType())) \
                       .withColumn("Longitud", col("Longitud").cast(DoubleType()))

# Eliminar duplicados (considerando todas las columnas)
df_clean = df_clean.dropDuplicates()

print("\n" + "="*80)
print("PROCESAMIENTO POR LOTES - DATOS METEOROLÓGICOS")
print("="*80)

print("\n[1] MUESTRA DE DATOS LIMPIOS (primeras 10 filas):")
print("-"*80)
df_clean.select("FechaObservacion", "ValorObservado", "DescripcionSensor", "Departamento", "Municipio").show(10, truncate=False)

print("\n[2] ESTADÍSTICAS DIARIAS POR SENSOR (primeras 20):")
print("-"*80)
daily_stats = df_clean.groupBy("anio", "mes", "dia", "DescripcionSensor") \
    .agg(
        avg("ValorObservado").alias("promedio"),
        min("ValorObservado").alias("minimo"),
        max("ValorObservado").alias("maximo"),
        stddev("ValorObservado").alias("desviacion"),
        count("ValorObservado").alias("registros")
    ) \
    .orderBy("anio", "mes", "dia", "DescripcionSensor")
daily_stats.show(20, truncate=False)

print("\n[3] ESTADÍSTICAS MENSUALES POR DEPARTAMENTO (primeras 20):")
print("-"*80)
monthly_dep = df_clean.groupBy("anio", "mes", "Departamento") \
    .agg(
        avg("ValorObservado").alias("promedio"),
        min("ValorObservado").alias("minimo"),
        max("ValorObservado").alias("maximo"),
        count("ValorObservado").alias("registros")
    ) \
    .orderBy("anio", "mes", "Departamento")
monthly_dep.show(20, truncate=False)

print("\n[4] DETECCIÓN DE OUTLIERS (percentiles 1% y 99% por sensor):")
print("-"*80)
# Calcular percentiles solo para sensores con suficientes datos
outlier_thresholds = df_clean.groupBy("DescripcionSensor") \
    .agg(
        percentile_approx("ValorObservado", 0.01).alias("p01"),
        percentile_approx("ValorObservado", 0.99).alias("p99")
    )

df_with_outliers = df_clean.join(outlier_thresholds, on="DescripcionSensor", how="left") \
    .withColumn("es_outlier",
                when((col("ValorObservado") < col("p01")) | (col("ValorObservado") > col("p99")), 1).otherwise(0))

outliers_count = df_with_outliers.groupBy("DescripcionSensor") \
    .agg(
        sum("es_outlier").alias("num_outliers"),
        count("ValorObservado").alias("total")
    ) \
    .withColumn("pct_outliers", (col("num_outliers") / col("total")) * 100) \
    .orderBy(col("pct_outliers").desc())
outliers_count.show(20, truncate=False)

print("\n[5] RESUMEN GENERAL:")
print("-"*80)
total_original = df_raw.count()
total_limpios = df_clean.count()
print(f"Registros originales: {total_original}")
print(f"Registros después de limpieza: {total_limpios}")
print(f"Registros eliminados: {total_original - total_limpios} ({((total_original - total_limpios)/total_original)*100:.2f}%)")

print("\n[6] TOP 10 SENSORES POR CANTIDAD DE REGISTROS:")
print("-"*80)
sensor_counts = df_clean.groupBy("DescripcionSensor").count().orderBy(col("count").desc())
sensor_counts.show(10, truncate=False)

# Mostrar distribución de tipos de sensor (opcional)
print("\n[7] DISTRIBUCIÓN POR TIPO DE SENSOR (PRIMEROS 10):")
print("-"*80)
tipo_sensor = df_clean.groupBy("DescripcionSensor", "UnidadMedida").count().orderBy(col("count").desc())
tipo_sensor.show(10, truncate=False)

spark.stop()
