from kafka import KafkaConsumer
import json
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")

# Kafka Parameters
broker = 'localhost:9092'
input_topic = 'eco-news'

# Create a consumer to listen to the first input topic
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=[broker],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Boucle principale pour lire les messages

eco_articles = []
try:
    print("Démarrage du consommateur. En attente de messages...")
    for message in consumer:
        if 'fin_de_vague' in message.value and message.value['fin_de_vague']:
            print("Fin de vague de messages")

            # Créer un DataFrame Pandas directement avec les données reçues
            df_eco = pd.DataFrame(eco_articles)

            # Créer une liste des titres d'articles
            top_headlines = df_eco["title"].tolist()

            # Tokeniser les titres d'articles et les faire passer à travers le modèle
            inputs = tokenizer(top_headlines, padding=True, truncation=True, return_tensors='pt')
            outputs = model(**inputs)

            # Calculer les prédictions de notre modèle
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

            positive = predictions[:, 0].tolist()
            negative = predictions[:, 1].tolist()
            neutral = predictions[:, 2].tolist()

            # Créer un tableau avec les données
            table = {
                'Headline': top_headlines,
                "Positive": positive,
                "Negative": negative,
                "Neutral": neutral
            }

            # Créer un DataFrame à partir du tableau
            df_eco_news = pd.DataFrame(table, columns=["Headline", "Positive", "Negative", "Neutral"])
            
            # Enregistrer le DataFrame directement dans un fichier CSV
            df_eco_news.to_csv('eco_articles.csv', index=False)
            
            # Réinitialiser eco_articles pour la prochaine vague de messages
            eco_articles = []
            
            print("En attente de messages...")
        else:
            article = message.value
            eco_articles.append(article)

except Exception as e:
    print(f"Une erreur s'est produite : {str(e)}")

except KeyboardInterrupt:
    print("Arrêt du consommateur")

consumer.close()