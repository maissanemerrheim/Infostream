from kafka import KafkaConsumer
import json

# Kafka Parameters
broker = 'localhost:9092'
input_topic = 'tech-news'

# Create a consumer to listen to the first input topic
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=[broker],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Boucle principale pour lire les messages
science_articles = []
try:
    print("Démarrage du consommateur. En attente de messages...")
    for message in consumer:
        article = message.value
        print(f'Reçu : {article["title"]}')
        article = message.value
        science_articles.append(article)
        if len(science_articles) >= 10:  # Limiter à 10 articles
            break

except KeyboardInterrupt:
    print("Arrêt du consommateur")

consumer.close()

with open('science_articles.json', 'w') as file:
    json.dump(science_articles, file)
