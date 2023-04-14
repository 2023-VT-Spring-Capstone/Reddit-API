from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from textblob import TextBlob
import json
# Function to perform sentiment analysis

def analyze_sentiment(text):
    return TextBlob(text).sentiment.polarity

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RedditSentimentAnalysis") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("TRACE")

# Initialize StreamingContext with a batch interval of 10 second
ssc = StreamingContext(spark.sparkContext, batchDuration=10)

# Connect to Kafka and subscribe to the topics
# kafka_params = {
#     "metadata.broker.list": "localhost:9092,localhost:9093,localhost:9094"}
kafka_params = {
    "zookeeper.connect": "localhost:2181",  # Update this with your Zookeeper host and port
    "group.id": "reddit-sentiment-group",
    "auto.offset.reset": "smallest"
}
kafka_topics = ["DWD_TOP_LOG", "DWD_HOT_LOG",
                "DWD_CONTROVERSIAL_LOG", "DWD_NEW_LOG"]

# kafka_stream = KafkaUtils.createDirectStream(
#     ssc,
#     kafka_topics,
#     kafka_params,
#     valueDecoder=lambda x: x.decode("utf-8")
# )

# # create a KafkaConsumer object
# consumer = KafkaConsumer(
#     kafka_topics,
#     kafka_params,
#     auto_offset_reset='earliest',
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )

kafka_stream = KafkaUtils.createStream(
    ssc,
    kafka_params["zookeeper.connect"],
    kafka_params["group.id"],
    {topic: 1 for topic in kafka_topics}
)


# # read messages from Kafka
# for message in consumer:
#     # do some processing on the message
#     processed_message = sentimeent_analysis(message.get("body"))

#     # create a DataFrame from the processed message
#     df = spark.createDataFrame([processed_message])

#     # write the DataFrame to an output sink
#     df.write.mode("append").parquet("path/to/output")


# Process the Kafka stream
lines = kafka_stream.map(lambda x: json.loads(x[1]))
bodies = lines.map(lambda x: x.get("body"))
sentiments = bodies.map(lambda x: analyze_sentiment(x))

lines.pprint()
sentiments.pprint()
# Calculate and display the average sentiment score over a window of time
 

# Calculate and save the average sentiment score to a text file
def save_avg_sentiment(rdd):
    if not rdd.isEmpty():
        avg_sentiment = rdd.mean()
        with open("result/sentiment_scores.txt", "a") as f:
            f.write(f"Average sentiment score: {avg_sentiment}\n")

sentiments.foreachRDD(save_avg_sentiment)

# Start the streaming context and await termination
ssc.start()
ssc.awaitTermination()
