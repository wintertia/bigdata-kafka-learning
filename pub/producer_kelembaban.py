import json
import time
import random
from kafka import KafkaProducer

KAFKA_TOPIC_NAME = "sensor-kelembaban-gudang"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

GUDANG_IDS = ["G1", "G2", "G3"]

if __name__ == "__main__":
    print("Memulai Kafka Producer Kelembaban...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        while True:
            gudang_id = random.choice(GUDANG_IDS)
            # Simulasikan kelembaban (misal antara 60 dan 80, dengan occasional spikes)
            kelembaban = random.randint(60, 75)
            if random.random() < 0.1: # 10% chance of higher humidity
                kelembaban = random.randint(71, 85)

            message = {
                "gudang_id": gudang_id,
                "kelembaban": kelembaban,
                "timestamp": time.time()
            }
            print(f"Mengirim data kelembaban: {message}")
            producer.send(KAFKA_TOPIC_NAME, message)
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer Kelembaban dihentikan.")
    finally:
        producer.close()
        print("Koneksi Producer Kelembaban ditutup.")