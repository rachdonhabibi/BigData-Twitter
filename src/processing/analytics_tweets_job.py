from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count as F_count,
    sum as F_sum,
    explode,
    size,
)

MONGO_URI = "mongodb://root:password123@mongodb:27017/reddit_data?authSource=admin"


def main():
    spark = (
        SparkSession.builder.appName("AnalyticsUkraineTweets")
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1) Lire la table nettoyée
    df = (
        spark.read.format("mongodb")
        .option("database", "reddit_data")
        .option("collection", "tweets_clean")
        .load()
    )

    # Ajout d'une colonne jour (date sans heure)
    df = df.withColumn("day", to_date(col("date")))
    df = df.filter(col("day").isNotNull())

    # -------------------------------------------------
    # KPI 1 : activité par jour -> tweets_kpi_daily
    # -------------------------------------------------
    df_daily = (
        df.groupBy("day")
        .agg(
            F_count("*").alias("tweets_count"),
            F_sum("engagement").alias("engagement_sum"),
        )
        .withColumnRenamed("day", "date")
    )

    (
        df_daily.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "tweets_kpi_daily")
        .save()
    )

    # -------------------------------------------------
    # KPI 2 : activité par jour et langue -> tweets_kpi_daily_lang
    # -------------------------------------------------
    df_daily_lang = (
        df.groupBy("day", "lang")
        .agg(
            F_count("*").alias("tweets_count"),
            F_sum("engagement").alias("engagement_sum"),
        )
        .withColumnRenamed("day", "date")
    )

    (
        df_daily_lang.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "tweets_kpi_daily_lang")
        .save()
    )

    # -------------------------------------------------
    # KPI 3 : stats par hashtag -> tweets_kpi_hashtags
    # -------------------------------------------------
    df_hashtags = df.filter(size(col("hashtags_array")) > 0)

    df_hashtags = (
        df_hashtags.select(
            explode(col("hashtags_array")).alias("hashtag"),
            col("engagement"),
        )
        .groupBy("hashtag")
        .agg(
            F_count("*").alias("tweets_count"),
            F_sum("engagement").alias("engagement_sum"),
        )
    )

    (
        df_hashtags.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "tweets_kpi_hashtags")
        .save()
    )

    # -------------------------------------------------
    # KPI 4 : stats par utilisateur -> tweets_kpi_users
    # -------------------------------------------------
    df_users = df.filter(col("username").isNotNull() & (col("username") != ""))

    df_users = (
        df_users.groupBy("username")
        .agg(
            F_count("*").alias("tweets_count"),
            F_sum("engagement").alias("engagement_sum"),
        )
    )

    (
        df_users.write.format("mongodb")
        .mode("overwrite")
        .option("database", "reddit_data")
        .option("collection", "tweets_kpi_users")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
