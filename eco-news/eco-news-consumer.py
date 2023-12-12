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
        article = message.value
        print(f'Reçu : {article["title"]}')
        article = message.value
        eco_articles.append(article)
        if len(eco_articles) >= 10:  # Limiter à 10 articles
            break

except KeyboardInterrupt:
    print("Arrêt du consommateur")

with open('eco_articles.json', 'w') as file:
    json.dump(eco_articles, file)

# Load the data 
with open('eco_articles.json', 'r') as file:
    json_data = file.read()

# Convert the data into a DataFrame for better manipulation
data_eco = pd.read_json(json_data)
df_eco = pd.DataFrame(data_eco)

# Create a list of the article titles 
top_headlines = []
for i in range(len(df_eco)) : 
    top_headlines.append(df_eco["title"][i])

# Tokenize the article titles and run them through the model 
inputs = tokenizer(top_headlines, padding = True, truncation = True, return_tensors='pt')
outputs = model(**inputs)

# Compute the predictions of our model 
predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

positive = predictions[:, 0].tolist()
negative = predictions[:, 1].tolist()
neutral = predictions[:, 2].tolist()


table = {'Headline':top_headlines,
         "Positive":positive,
         "Negative":negative, 
         "Neutral":neutral}
      
df_eco_news = pd.DataFrame(table, columns = ["Headline", "Positive", "Negative", "Neutral"])
print(df_eco_news)

consumer.close()