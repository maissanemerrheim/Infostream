import json
import pandas as pd
import argparse
from kafka import KafkaConsumer, KafkaAdminClient
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from io import StringIO


class EcoNewsConsumer:
    def __init__(self, broker, input_topic):
        """
        Initialize the EcoNewsConsumer.

        Parameters:
        broker (str): The Kafka broker address.
        input_topic (str): The topic to subscribe to and consume messages from.
        """
        self.broker = broker
        self.input_topic = input_topic
        self.ensure_topic_exists()
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=[broker],
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "ProsusAI/finbert")
        self.eco_articles = []

    def ensure_topic_exists(self):
        """
        Verify that the specified Kafka topic exists.
        If not, raise an exception.
        """
        admin_client = KafkaAdminClient(bootstrap_servers=self.broker)
        topic_list = admin_client.list_topics()
        if self.input_topic not in topic_list:
            raise Exception(
                f"The topic '{self.input_topic}' does not exist.")

    def process_messages(self):
        """
        Continuously consume messages from the Kafka topic,
        performing sentiment analysis on each batch received.
        """
        print("Consumer starting. Awaiting messages...")
        for message in self.consumer:
            if 'fin_de_vague' in message.value \
                    and message.value['fin_de_vague']:
                self.perform_sentiment_analysis()
            else:
                article = message.value
                self.eco_articles.append(article)

    def perform_sentiment_analysis(self):
        """
        Perform sentiment analysis on the articles, using the FinBERT model.
        Outputs the sentiment analysis to a CSV file specific to the topic.
        """
        print("End of message batch")
        with open(f'eco_articles_{self.input_topic}.json', 'w') as file:
            json.dump(self.eco_articles, file)

        # Load the data
        with open(f'eco_articles_{self.input_topic}.json', 'r') as file:
            json_data = file.read()
        data_eco = pd.read_json(StringIO(json_data))
        df_eco = pd.DataFrame(data_eco)

        # Sentiment analysis
        top_headlines = []
        for i in range(len(df_eco)):
            top_headlines.append(df_eco["title"][i])

        inputs = self.tokenizer(top_headlines,
                                padding=True,
                                truncation=True,
                                return_tensors='pt')
        outputs = self.model(**inputs)

        predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)

        positive = predictions[:, 0].tolist()
        negative = predictions[:, 1].tolist()
        neutral = predictions[:, 2].tolist()

        table = {'Headline': top_headlines,
                 "Positive": positive,
                 "Negative": negative,
                 "Neutral": neutral}

        df_eco_news = pd.DataFrame(
            table,
            columns=["Headline", "Positive", "Negative", "Neutral"])
        print(df_eco_news)
        df_eco_news.to_csv(f"./articles_eco_{self.input_topic}.csv")

        # Reset for the next batch
        self.eco_articles = []
        print("Waiting for messages...")

    def run(self):
        try:
            self.process_messages()
        except KeyboardInterrupt:
            print("Stopping the consumer")
            self.consumer.close()


if __name__ == "__main__":
    # These could be loaded from command line arguments,
    # environment variables, or a config file
    broker = 'localhost:9092'

    # Setup argparse for command line topic selection
    parser = argparse.ArgumentParser(description='Eco News Producer Script')
    parser.add_argument('topic', help='Specify the topic for the consumer')
    args = parser.parse_args()

    # Initialize and run the consumer
    consumer = EcoNewsConsumer(broker, args.topic)
    consumer.run()
