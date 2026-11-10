import json
from kafka import KafkaConsumer
from hdfs import InsecureClient

TOPIC = "weather_transformed"

client = InsecureClient("http://namenode:9870", user="root")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="hdfs-writer",
    value_deserializer=lambda v: v.decode("utf-8"),
)

print(f"Ecriture HDFS depuis {TOPIC}...")
for message in consumer:
    ligne = message.value
    data = json.loads(ligne)
    chemin = f"/hdfs-data/{data['country']}/{data['city']}/alerts.json"

    client.makedirs(f"/hdfs-data/{data['country']}/{data['city']}")
    deja_la = client.status(chemin, strict=False) is not None
    client.write(chemin, data=ligne + "\n", encoding="utf-8", append=deja_la)
    print(f"Ajoute dans {chemin}")