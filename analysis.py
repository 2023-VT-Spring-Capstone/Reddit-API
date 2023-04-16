from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob
import json
from sentiment_analysis import sentiment_analysis
from elasticsearch import Elasticsearch

bootstrap_servers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
input_topic = 'DWD_TOP_LOG'
output_topic = 'DWD_ANALYZED_LOG'
consumer_group = 'my_consumer_group'
es = Elasticsearch(['http://localhost:9200'],
                   http_auth =('elastic', 'lIaOuoKHcJcM173Ei8U7'))



# Create Kafka consumer and producer
consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=bootstrap_servers,
    group_id=consumer_group,
    auto_offset_reset='earliest',  # Start reading from the beginning of the topic
    # enable_auto_commit=True,  # Enable automatic offset commit
    value_deserializer=lambda m: m.decode('utf-8'),  # Decode message values as UTF-8 strings
)

consumer.subscribe([input_topic])

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))


def main():
    for message in consumer:
        message_value = message.value
        print(message_value)
        sentiment_analysis_result = sentiment_analysis(json.loads(message_value))
        print(sentiment_analysis_result)
        if sentiment_analysis_result:
            producer.send(output_topic, value=sentiment_analysis_result)
            es.index(index='stock_sentiment_analysis', document=sentiment_analysis_result)
if __name__ == "__main__":
    main()
