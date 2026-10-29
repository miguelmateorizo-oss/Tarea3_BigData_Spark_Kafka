from kafka import KafkaProducer
import json
import time

# Conectar Python con Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Datos de ventas
ventas = [
    {"producto": "Laptop", "cantidad": 2, "precio": 800},
    {"producto": "Mouse", "cantidad": 5, "precio": 20},
    {"producto": "Teclado", "cantidad": 3, "precio": 50},
    {"producto": "Monitor", "cantidad": 2, "precio": 300},
    {"producto": "Audifonos", "cantidad": 4, "precio": 75}
]

print("INICIANDO ENVIO DE VENTAS A KAFKA")

for venta in ventas:
    producer.send("ventas", value=venta).get(timeout=10)
    print("Venta enviada:", venta)
    time.sleep(2)

producer.flush()
producer.close()

print("TODAS LAS VENTAS FUERON ENVIADAS")

