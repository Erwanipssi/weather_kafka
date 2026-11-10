import json
import time
import requests
from kafka import KafkaProducer

TOPIC = "weather_stream"

CITIES = [
    {"city": "Paris", "country": "France", "latitude": 48.8566, "longitude": 2.3522},
    {"city": "Seville", "country": "Spain", "latitude": 37.3891, "longitude": -5.9845},
    {"city": "Dubai", "country": "United Arab Emirates", "latitude": 25.2048, "longitude": 55.2708},
    {"city": "Reykjavik", "country": "Iceland", "latitude": 64.1466, "longitude": -21.9426},
    {"city": "Wellington", "country": "New Zealand", "latitude": -41.2865, "longitude": 174.7762},
]

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def fetch_weather(latitude, longitude):
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        "&current=temperature_2m,wind_speed_10m,weather_code"
        "&wind_speed_unit=ms"
    )
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    return response.json()

print(f"Producteur en cours, topic {TOPIC}")
while True:
    for city in CITIES:
        data = fetch_weather(city["latitude"], city["longitude"])
        data["city"] = city["city"]
        data["country"] = city["country"]
        producer.send(TOPIC, value=data)
        print(f"Envoye : {city['city']}")
    producer.flush()
    time.sleep(30)