from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    TimestampType,
)

# Schéma du JSON envoyé par le producteur Kafka
tweet_schema = StructType(
    [
        StructField("id", LongType(), nullable=False),
        StructField("date", StringType(), nullable=True),  # sera casté en timestamp
        StructField("user", StringType(), nullable=True),
        StructField("content", StringType(), nullable=True)
        StructField("replyCount", IntegerType(), nullable=True),
        StructField("retweetCount", IntegerType(), nullable=True),
        StructField("likeCount", IntegerType(), nullable=True),
        StructField("quoteCount", IntegerType(), nullable=True),
        StructField("conversationId", LongType(), nullable=True),
        StructField("inReplyToTweetId", LongType(), nullable=True),
        StructField("inReplyToUser", StringType(), nullable=True),
        StructField("mentionedUsers", StringType(), nullable=True),
        StructField("hashtags", StringType(), nullable=True),
        StructField("lang", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True),
    ]
)

# URI Mongo avec authentification root
MONGO_URI = "mongodb://root:password123@mongodb:27017/reddit_data?authSource=admin"


def main():
    # 1) SparkSession avec config MongoDB
    spark = (
        SparkSession.builder.appName("UkraineTweetsStreamingToMongo")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2) Lecture du flux Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "ukraine_tweets")
        .option("startingOffsets", "earliest")
        .load()
    )

    # 3) Parser le JSON dans la colonne value
    value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df = value_df.select(
        from_json(col("json_str"), tweet_schema).alias("data")
    ).select("data.*")

    # 4) Caster les types + ajouter l'engagement
    typed_df = (
        parsed_df.withColumn("date_ts", col("date").cast(TimestampType()))
        .withColumn("replyCount", col("replyCount").cast(IntegerType()))
        .withColumn("retweetCount", col("retweetCount").cast(IntegerType()))
        .withColumn("likeCount", col("likeCount").cast(IntegerType()))
        .withColumn("quoteCount", col("quoteCount").cast(IntegerType()))
    )

    enriched_df = typed_df.withColumn(
        "engagement",
        (
            col("replyCount").cast("long")
            + col("retweetCount").cast("long")
            + col("likeCount").cast("long")
            + col("quoteCount").cast("long")
        ),
    )

    # 5) Fonction appelée pour chaque micro-batch => écriture Mongo
    def write_to_mongo(batch_df, batch_id):
        (
            batch_df.drop("date")  # on remplace par date_ts
            .withColumnRenamed("date_ts", "date")
            .write.format("mongodb")
            .mode("append")
            .option("uri", MONGO_URI)
            .option("database", "reddit_data")
            .option("collection", "tweets_stream")
            .save()
        )

    # 6) Lancer le streaming avec foreachBatch (nouveau checkpoint)
    query = (
        enriched_df.writeStream.outputMode("append")
        .foreachBatch(write_to_mongo)
        .option("checkpointLocation", "/tmp/checkpoints/ukraine_tweets_mongo_v2")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()