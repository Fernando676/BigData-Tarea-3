"""
Productor Kafka que envía cada fila del CSV al tópico 'datos_clima'.
Adaptado al formato de columnas y limpieza de valores numéricos.
"""

from kafka import KafkaProducer
import pandas as pd
import json
import time
import sys

# Configuración
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'datos_clima'
CSV_PATH = "/home/vboxuser/57sv-p2fu.csv"
SLEEP_SEC = 0.1  # Tiempo entre envíos para simular flujo real

def create_producer():
    """Crea y retorna un productor Kafka con serialización JSON."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        print(f"Productor conectado a {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"Error al conectar con Kafka: {e}")
        sys.exit(1)

def clean_numeric(x):
    """
    Convierte valores numéricos con comas (ej. "1,006.80") a float.
    Si no es convertible, devuelve el valor original.
    """
    if pd.isna(x):
        return None
    if isinstance(x, str):
        # Eliminar comas de miles
        x = x.replace(',', '')
        try:
            return float(x)
        except ValueError:
            return x
    return x

def send_data(producer):
    """Lee el CSV, limpia los datos y envía cada fila al topic."""
    try:
        df = pd.read_csv(CSV_PATH, encoding='utf-8')
    except FileNotFoundError:
        print(f"Archivo no encontrado: {CSV_PATH}")
        sys.exit(1)

    # Limpiar la columna de valor observado (eliminar comas y convertir a número)
    if 'ValorObservado' in df.columns:
        df['ValorObservado'] = df['ValorObservado'].apply(clean_numeric)

    # Reemplazar NaN por None (para que JSON los serialice como null)
    df = df.where(pd.notnull(df), None)

    print(f"Iniciando envío de {len(df)} mensajes al tópico '{TOPIC}'...")

    for idx, row in df.iterrows():
        message = row.to_dict()
        try:
            producer.send(TOPIC, value=message)
            # Mostrar progreso cada 100 mensajes
            if idx % 100 == 0:
                print(f"Enviados {idx+1}/{len(df)} mensajes")
        except Exception as e:
            print(f"Error al enviar mensaje {idx}: {e}")
        time.sleep(SLEEP_SEC)

    producer.flush()
    print("Todos los mensajes enviados. Productor cerrado.")

if __name__ == "__main__":
    prod = create_producer()
    send_data(prod)
    prod.close()
