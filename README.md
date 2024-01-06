# EcoNews Sentiment Analysis with Kafka Integration

This project consists of two Python scripts: one for producing news articles from an external API to a Kafka topic, and the other for consuming and analyzing those articles for sentiment analysis. This project also includes a notebook for visualizing results. You'll also find 2 folders for results obtained with articles on cryptocurrency and stock market indices

## Prerequisites

Before running the producer and consumer scripts, make sure you have the following prerequisites installed:

- Python 3.x
- Kafka Server (with ZooKeeper)

To start ZooKeeper, use the following command:
  ```bash
  ./bin/zookeeper-server-start.sh config/zookeeper.properties
  ```

To start Kafka Server, use the following command:
```bash
./bin/kafka-server-start.sh config/server.properties
```

Make sure to adapt the paths and configurations according to your actual Kafka setup.

You will also need an API key for the news service (https://newsapi.org/)

## Installation

Clone the repository:

```bash
   git clone https://github.com/maissanemerrheim/Infostream.git
```

## USAGE
### Producer
To run the producer script, use the following command:
```bash
    python eco_news_producer.py <keywords>
```

Replace <keywords> with the keywords you want to use for news searching. This script will continuously fetch news articles based on the provided keywords and send them to a Kafka topic.

WARNING: It's essential to select appropriate keywords related to economic topics because we use the FinBERT model for sentiment analysis, a pre-trained transformer-based model designed specifically for financial sentiment analysis.


### Consumer
To run the consumer script, use the following command:
```bash
    python eco_news_consumer.py <topic>
```

Replace <topic> with the Kafka topic from which you want to consume messages (topic_keywords with _ if you have spaces). The consumer will perform sentiment analysis on the received articles using the FinBERT model and save the results in CSV files specific to the topic.

## Authors and acknowledgment
LAMJOUN Jihane
MERRHEIM Maïssane
