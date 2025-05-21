import json
import time
import random
from kafka import KafkaProducer

KAFKA_TOPIC_NAME = "sensor-suhu-gudang"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'

GUDANG_IDS = ["G1", "G2", "G3"]

if __name__ == "__main__":
    print("Memulai Kafka Producer Suhu...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        while True:
            gudang_id = random.choice(GUDANG_IDS)
            # Simulasikan suhu (misal antara 70 dan 90, dengan occasional spikes)
            suhu = random.randint(70, 85)
            if random.random() < 0.1: # 10% chance of higher temp
                suhu = random.randint(81, 90)

            message = {
                "gudang_id": gudang_id,
                "suhu": suhu,
                "timestamp": time.time()
            }
            print(f"Mengirim data suhu: {message}")
            producer.send(KAFKA_TOPIC_NAME, message)
            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer Suhu dihentikan.")
    finally:
        producer.close()
        print("Koneksi Producer Suhu ditutup.")