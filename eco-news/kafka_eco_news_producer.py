import time
import json
import requests
import argparse
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


class EcoNewsProducer:
    def __init__(self, broker, api_key, keywords):
        """
        Initialize the EcoNewsProducer.

        Parameters:
        broker (str): The Kafka broker address.
        api_key (str): API key for the news service.
        keywords (str): Keywords for the news search.
        """
        self.broker = broker
        self.topic = f"topic_{keywords.replace(' ', '_')}"
        self.api_key = api_key
        self.keywords = keywords
        self.producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def create_topic(self):
        """
        Create a topic in Kafka if it does not already exist.
        The topic name is derived from the keyword.
        """
        admin_client = KafkaAdminClient(bootstrap_servers=self.broker)

        topic_list = []
        topic_list.append(
            NewTopic(
                name=self.topic,
                num_partitions=1,
                replication_factor=1)
        )

        try:
            admin_client.create_topics(
                new_topics=topic_list,
                validate_only=False
            )
            print(f"Topic {self.topic} created")
        except TopicAlreadyExistsError:
            print(f"Topic {self.topic} already exists")

    def fetch_and_send_articles(self):
        """
        Fetch articles from the news API using the specified keywords
        and send them to the Kafka topic.
        """
        base_url = "https://newsapi.org/v2/everything"
        params = f"?language=en&q={self.keywords}&apiKey={self.api_key}"
        url = base_url + params
        response = requests.get(url)

        if response.status_code == 200:
            articles = response.json().get('articles', [])
            for article in articles:
                self.producer.send(self.topic, article)
            print("Data sent to Kafka topic")
            self.producer.send(self.topic, {"fin_de_vague": True})
            print("End of batch message sent")
        else:
            print(f"Error fetching articles : {response.status_code}")

    def run(self):
        """
        Run the EcoNewsProducer.
        This involves creating the topic (if necessary)
        and entering a loop to continuously fetch and send articles every 40s.
        """
        try:
            self.create_topic()
            while True:
                self.fetch_and_send_articles()
                time.sleep(20)
        except KeyboardInterrupt:
            print("Stopping the producer")


if __name__ == "__main__":
    # These could be loaded from command line arguments,
    # environment variables, or a config file
    broker = 'localhost:9092'
    api_key = 'a61ddae6d1f04e839adb6268a8451c28'

    # Setup argparse for command line topic selection
    parser = argparse.ArgumentParser(description='Eco News Producer Script')
    parser.add_argument('keywords', help='Keywords for the news search')
    args = parser.parse_args()

    # Initialize and run the producer
    producer = EcoNewsProducer(broker, api_key, args.keywords)
    producer.run()
