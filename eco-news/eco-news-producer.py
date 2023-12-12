import time
import json
import requests
from kafka import KafkaProducer

# Kafka Parameters
broker = 'localhost:9092'  
topic = 'eco-news'

# Configuration du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Remplacez par l'adresse de votre serveur Kafka
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializer pour convertir les messages en JSON
)

# Récupération et envoi des articles
def fetch_and_send_articles():
    url = 'https://newsapi.org/v2/top-headlines?country=us&category=business&apiKey=a61ddae6d1f04e839adb6268a8451c28'
    response = requests.get(url)
    if response.status_code == 200:
        articles = response.json().get('articles', [])
        for article in articles:
            producer.send("eco-news", article)
        print("Data sent to Kafka topic")
    else:
        print(f"Erreur lors de la récupération des articles : {response.status_code}")

# Boucle principale
try:
    while True:
        fetch_and_send_articles()
        time.sleep(10)  # Pause de 10 secondes
except KeyboardInterrupt:
    print("Arrêt du script")
