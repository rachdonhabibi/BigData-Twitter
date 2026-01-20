from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    lit,
    sum as F_sum,
)

MONGO_URI = "mongodb://root:password123@mongodb:27017/reddit_data?authSource=admin"


def main():
    spark = (
        SparkSession.builder.appName("SocialGraphUkraineTweets")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Charger les tweets nettoyés
    df = (
        spark.read.format("mongodb")
        .option("database", "reddit_data")
        .option("collection", "tweets_clean")
        .load()
    )

    # Garder seulement les tweets avec auteur connu
    df = df.filter(col("username").isNotNull() & (col("username") != ""))

    # -------------------------------------------------
    # 2) Arêtes "mention" : username -> chaque mention
    # -------------------------------------------------
    mentions_edges = (
        df.select(
            col("username").alias("source_user"),
            explode("mentions_array").alias("target_user"),
        )
        .filter(col("target_user").isNotNull() & (col("target_user") != ""))
        .withColumn("type", lit("mention"))
        .withColumn("weight", lit(1))
    )

    # -------------------------------------------------
    # 3) Arêtes "reply" : auteur du reply -> auteur du tweet original
    # -------------------------------------------------
    # A = replies
    df_a = df.select(
        col("id").alias("tweet_id"),
        col("username").alias("source_user"),
        col("inReplyToTweetId"),
    )

    # B = tweets originaux
    df_b = df.select(
        col("id").alias("orig_tweet_id"),
        col("username").alias("target_user"),
    )

    reply_edges = (
        df_a.join(
            df_b,
            df_a["inReplyToTweetId"] == df_b["orig_tweet_id"],
            how="inner",
        )
        .select("source_user", "target_user")
        .filter(
            col("source_user").isNotNull()
            & (col("source_user") != "")
            & col("target_user").isNotNull()
            & (col("target_user") != "")
        )
        .withColumn("type", lit("reply"))
        .withColumn("weight", lit(1))
    )

    # -------------------------------------------------
    # 4) Union + agrégation des arêtes
    # -------------------------------------------------
    edges_raw = mentions_edges.unionByName(reply_edges, allowMissingColumns=True)

    edges_agg = (
        edges_raw.groupBy("source_user", "target_user", "type")
        .agg(F_sum("weight").alias("weight"))
    )

    # Écriture dans Mongo : collection user_edges
    (
        edges_agg.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "user_edges")
        .save()
    )

    # -------------------------------------------------
    # 5) Table des nœuds (utilisateurs) de base
    # -------------------------------------------------
    # - tous les auteurs
    authors = df.select(col("username").alias("username")).distinct()

    # - tous les utilisateurs mentionnés
    mentions = (
        df.select(explode("mentions_array").alias("username"))
        .filter(col("username").isNotNull() & (col("username") != ""))
        .distinct()
    )

    nodes = authors.unionByName(mentions, allowMissingColumns=True).distinct()

    # Option : enrichir avec les stats d'engagement depuis tweets_kpi_users
    try:
        kpi_users = (
            spark.read.format("mongodb")
            .option("database", "reddit_data")
            .option("collection", "tweets_kpi_users")
            .load()
        )
        user_nodes = (
            nodes.join(kpi_users, on="username", how="left")
            .select(
                "username",
                "tweets_count",
                "engagement_sum",
            )
        )
    except Exception:
        # si la collection n'existe pas, on garde juste les usernames
        user_nodes = nodes.select("username")

    (
        user_nodes.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "user_nodes")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
