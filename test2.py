from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from textblob import TextBlob

# Function to perform sentiment analysis
def analyze_sentiment(text):
    analysis = TextBlob(text)
    sentiment = analysis.sentiment.polarity
    return str(sentiment)

# Register the UDF to be used in Spark
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RedditSentimentAnalysis") \
    .getOrCreate()

# Initialize StreamingContext with a batch interval of 1 second
ssc = StreamingContext(spark.sparkContext, 1)

# Connect to Kafka and subscribe to the topics
kafka_params = {"metadata.broker.list": "localhost:9092,localhost:9093,localhost:9094"}
kafka_topics = ["DWD_TOP_LOG", "DWD_HOT_LOG", "DWD_CONTROVERSIAL_LOG", "DWD_NEW_LOG"]

kafka_stream = KafkaUtils.createDirectStream(
    ssc,
    kafka_topics,
    kafka_params,
    valueDecoder=lambda x: x.decode("utf-8")
)

# Process the Kafka stream
def process_stream(rdd):
    if not rdd.isEmpty():
        # Convert RDD to DataFrame
        df = rdd.toDF(["value"])

        # Perform sentiment analysis on the "body" field
        df = df.selectExpr("json_tuple(value, 'body') as body")
        df = df.withColumn("sentiment", analyze_sentiment_udf(df["body"]))

        # Store the results
        df.write.mode("append").json("result")
        print("--------********************************************called")

kafka_stream.foreachRDD(process_stream)

# Start the streaming context and await termination
ssc.start()
ssc.awaitTermination(60)
