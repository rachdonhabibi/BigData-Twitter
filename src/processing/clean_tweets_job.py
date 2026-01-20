from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_extract,
    regexp_replace,
    split,
    trim,
    coalesce,
    lit,
    array_remove,
)
from pyspark.sql.types import LongType, IntegerType, TimestampType, ArrayType, StringType
from pyspark.sql.functions import udf
import ast

# URI MongoDB avec authentification root
MONGO_URI = "mongodb://root:password123@mongodb:27017/reddit_data?authSource=admin"


def extract_mentions(raw):
    """
    Parse le champ mentionedUsers (string représentant une liste de dicts)
    et renvoie une liste de usernames mentionnés.
    """
    if raw is None:
        return []
    try:
        data = ast.literal_eval(raw)
        # parfois un seul dict, parfois une liste
        if isinstance(data, dict):
            data = [data]
        usernames = []
        for u in data:
            if isinstance(u, dict) and "username" in u and u["username"]:
                usernames.append(str(u["username"]))
        return usernames
    except Exception:
        return []


extract_mentions_udf = udf(extract_mentions, ArrayType(StringType()))


def main():
    # 1) Créer une SparkSession configurée pour lire/écrire dans MongoDB
    spark = (
        SparkSession.builder.appName("CleanUkraineTweets")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2) Lire la collection brute 'tweets_stream'
    raw_df = (
        spark.read.format("mongodb")
        .option("database", "reddit_data")
        .option("collection", "tweets_stream")
        .load()
    )

    # 3) Colonnes utiles
    df = raw_df.select(
        "id",
        "date",
        "content",
        "lang",
        "replyCount",
        "retweetCount",
        "likeCount",
        "quoteCount",
        "conversationId",
        "inReplyToTweetId",
        "user",
        "mentionedUsers",
        "hashtags",
        "url",
    )

    # 4) Cast des types
    df = (
        df.withColumn("id", col("id").cast(LongType()))
        .withColumn("date", col("date").cast(TimestampType()))
        .withColumn("replyCount", col("replyCount").cast(IntegerType()))
        .withColumn("retweetCount", col("retweetCount").cast(IntegerType()))
        .withColumn("likeCount", col("likeCount").cast(IntegerType()))
        .withColumn("quoteCount", col("quoteCount").cast(IntegerType()))
        .withColumn("conversationId", col("conversationId").cast(LongType()))
        .withColumn("inReplyToTweetId", col("inReplyToTweetId").cast(LongType()))
    )

    # 5) Recalcul de l’engagement
    df = df.withColumn(
        "engagement",
        (
            coalesce(col("replyCount"), lit(0)).cast("long")
            + coalesce(col("retweetCount"), lit(0)).cast("long")
            + coalesce(col("likeCount"), lit(0)).cast("long")
            + coalesce(col("quoteCount"), lit(0)).cast("long")
        ),
    )

    # 6) Extraction d’un username propre (auteur du tweet)
    df = df.withColumn(
        "username",
        regexp_extract(col("user"), r"'username': '([^']+)'", 1),
    )

    # 7) Nettoyage des hashtags -> hashtags_array
    clean_hashtags_str = regexp_replace(
        regexp_replace(
            coalesce(col("hashtags"), lit("")),
            r"[\[\]'`]",
            "",
        ),
        r"\s+",
        "",
    )
    df = df.withColumn("hashtags_array", split(trim(clean_hashtags_str), ","))
    df = df.withColumn("hashtags_array", array_remove(col("hashtags_array"), ""))

    # 8) Extraction des mentions -> mentions_array
    df = df.withColumn("mentions_array", extract_mentions_udf(col("mentionedUsers")))

    # 9) Dédoublonnage + filtrage minimal
    df = df.dropDuplicates(["id"]).filter(col("content").isNotNull())

    # 10) Écriture dans la collection nettoyée
    (
        df.drop("user", "mentionedUsers", "hashtags")  # on garde hashtags_array & mentions_array
        .write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "tweets_clean")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
