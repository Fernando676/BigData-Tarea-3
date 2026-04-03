"""
Luis Fernando Ruiz
Materia Big Data
UNAD
Procesamiento por lotes de datos meteorológicos con limpieza completa.
Muestra en consola:
- Datos limpios (primeras filas)
- Estadísticas diarias por sensor
- Estadísticas mensuales por departamento
- Promedio de precipitación por departamento (ordenado de mayor a menor)
- Detección de outliers por sensor
- Resumen general
- Top 10 sensores por cantidad de registros
- Distribución por tipo de sensor
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth, hour,
    avg, min, max, count, stddev, when, lit, percentile_approx, sum, regexp_replace, desc
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

# 3. LIMPIEZA DE DATOS
# 3.1. Limpiar valores numéricos: eliminar comas de miles (ej. "1,006.80") y convertir a double
df_clean = df_raw.withColumn("ValorObservado_clean", 
                             regexp_replace(col("ValorObservado"), ",", "").cast(DoubleType())) \
                 .drop("ValorObservado") \
                 .withColumnRenamed("ValorObservado_clean", "ValorObservado")

# 3.2. Convertir fecha: formato "2026 Apr 01 02:37:00 PM" -> usar patrón "yyyy MMM dd hh:mm:ss a"
# Nota: Spark requiere que el mes esté en inglés (Apr, Mar, etc.)
df_clean = df_clean.withColumn("FechaObservacion", 
                               to_timestamp(col("FechaObservacion"), "yyyy MMM dd hh:mm:ss a")) \
                   .dropna(subset=["ValorObservado", "FechaObservacion"])

# 3.3. Extraer componentes de fecha (año, mes, día, hora)
df_clean = df_clean.withColumn("anio", year("FechaObservacion")) \
                   .withColumn("mes", month("FechaObservacion")) \
                   .withColumn("dia", dayofmonth("FechaObservacion")) \
                   .withColumn("hora", hour("FechaObservacion"))

# 3.4. Convertir latitud y longitud a double si existen
if "Latitud" in df_clean.columns and "Longitud" in df_clean.columns:
    df_clean = df_clean.withColumn("Latitud", col("Latitud").cast(DoubleType())) \
                       .withColumn("Longitud", col("Longitud").cast(DoubleType()))

# 3.5. Eliminar duplicados completos (considerando todas las columnas)
df_clean = df_clean.dropDuplicates()

print("\n" + "="*80)
print("PROCESAMIENTO POR LOTES - DATOS METEOROLÓGICOS")
print("="*80)

# 4. MUESTRA DE DATOS LIMPIOS
print("\n[1] MUESTRA DE DATOS LIMPIOS (primeras 10 filas):")
print("-"*80)
df_clean.select("FechaObservacion", "ValorObservado", "DescripcionSensor", "Departamento", "Municipio").show(10, truncate=False)

# 5. ESTADÍSTICAS DIARIAS POR SENSOR
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

# 6. ESTADÍSTICAS MENSUALES POR DEPARTAMENTO
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

# 7. PROMEDIO DE PRECIPITACIÓN POR DEPARTAMENTO (ORDENADO DE MAYOR A MENOR)
print("\n[4] PROMEDIO DE PRECIPITACIÓN POR DEPARTAMENTO (mayor a menor):")
print("-"*80)
# Filtrar sensores que miden precipitación (DescripcionSensor contiene "PRECIPITACI")
precip_df = df_clean.filter(col("DescripcionSensor").contains("PRECIPITACI"))
# Agrupar por departamento y calcular promedio
avg_precip = precip_df.groupBy("Departamento") \
    .agg(avg("ValorObservado").alias("promedio_precipitacion_mm")) \
    .orderBy(desc("promedio_precipitacion_mm"))  # Orden descendente
avg_precip.show(truncate=False)

# 8. DETECCIÓN DE OUTLIERS (percentiles 1% y 99% por sensor)
print("\n[5] DETECCIÓN DE OUTLIERS (percentiles 1% y 99% por sensor):")
print("-"*80)
# Calcular percentiles por sensor
outlier_thresholds = df_clean.groupBy("DescripcionSensor") \
    .agg(
        percentile_approx("ValorObservado", 0.01).alias("p01"),
        percentile_approx("ValorObservado", 0.99).alias("p99")
    )
# Marcar outliers
df_with_outliers = df_clean.join(outlier_thresholds, on="DescripcionSensor", how="left") \
    .withColumn("es_outlier",
                when((col("ValorObservado") < col("p01")) | (col("ValorObservado") > col("p99")), 1).otherwise(0))
# Contar outliers por sensor
outliers_count = df_with_outliers.groupBy("DescripcionSensor") \
    .agg(
        sum("es_outlier").alias("num_outliers"),
        count("ValorObservado").alias("total")
    ) \
    .withColumn("pct_outliers", (col("num_outliers") / col("total")) * 100) \
    .orderBy(col("pct_outliers").desc())
outliers_count.show(20, truncate=False)

# 9. RESUMEN GENERAL
print("\n[6] RESUMEN GENERAL:")
print("-"*80)
total_original = df_raw.count()
total_limpios = df_clean.count()
print(f"Registros originales: {total_original}")
print(f"Registros después de limpieza: {total_limpios}")
print(f"Registros eliminados: {total_original - total_limpios} ({((total_original - total_limpios)/total_original)*100:.2f}%)")

# 10. TOP 10 SENSORES POR CANTIDAD DE REGISTROS
print("\n[7] TOP 10 SENSORES POR CANTIDAD DE REGISTROS:")
print("-"*80)
sensor_counts = df_clean.groupBy("DescripcionSensor").count().orderBy(col("count").desc())
sensor_counts.show(10, truncate=False)

# 11. DISTRIBUCIÓN POR TIPO DE SENSOR Y UNIDAD DE MEDIDA
print("\n[8] DISTRIBUCIÓN POR TIPO DE SENSOR (primeros 10):")
print("-"*80)
tipo_sensor = df_clean.groupBy("DescripcionSensor", "UnidadMedida").count().orderBy(col("count").desc())
tipo_sensor.show(10, truncate=False)

# Finalizar sesión Spark
spark.stop()

