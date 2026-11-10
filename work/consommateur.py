from kafka import KafkaConsumer

TOPIC = "weather_stream"

consommateur = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="weather-consumer",
    value_deserializer=lambda v: v.decode("utf-8"),
)

print(f"En ecoute sur {TOPIC}...")
for message in consommateur:
    print(message.value)