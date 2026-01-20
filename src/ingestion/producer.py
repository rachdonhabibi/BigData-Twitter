import json
import os
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


DATA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "ukraine_war_clean.parquet")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ukraine_tweets")
def create_producer(max_retries: int = 10, delay_seconds: float = 5.0) -> KafkaProducer:
    """Crée un KafkaProducer avec quelques tentatives si Kafka n'est pas encore prêt."""

    attempt = 0
    while True:
        try:
            print(f"Connexion à Kafka ({KAFKA_BOOTSTRAP_SERVERS}), tentative {attempt + 1}/{max_retries}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
            )
            print("Connexion à Kafka réussie.")
            return producer
        except NoBrokersAvailable as e:
            attempt += 1
            if attempt >= max_retries:
                print("Échec de connexion à Kafka après plusieurs tentatives.")
                raise e
            print(f"Kafka non disponible, nouvelle tentative dans {delay_seconds} secondes...")
            time.sleep(delay_seconds)


def stream_tweets(batch_size: int = 1000, sleep_seconds: float = 0.5) -> None:
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"Fichier Parquet introuvable : {DATA_PATH}")

    print(f"Lecture des tweets nettoyés depuis : {DATA_PATH}")
    df = pd.read_parquet(DATA_PATH)
    print(f"Nombre total de tweets à streamer : {len(df)}")

    producer = create_producer()

    sent = 0
    for _, row in df.iterrows():
        event = {
            "id": int(row["id"]),
            "date": row["date"].isoformat() if pd.notna(row["date"]) else None,
            "user": row.get("user"),
            "content": row.get("content"),
            "replyCount": int(row["replyCount"]) if not pd.isna(row["replyCount"]) else 0,
            "retweetCount": int(row["retweetCount"]) if not pd.isna(row["retweetCount"]) else 0,
            "likeCount": int(row["likeCount"]) if not pd.isna(row["likeCount"]) else 0,
            "quoteCount": int(row["quoteCount"]) if not pd.isna(row["quoteCount"]) else 0,
            "conversationId": int(row["conversationId"]) if not pd.isna(row["conversationId"]) else None,
            "inReplyToTweetId": int(row["inReplyToTweetId"]) if not pd.isna(row["inReplyToTweetId"]) else None,
            "inReplyToUser": row.get("inReplyToUser"),
            "mentionedUsers": row.get("mentionedUsers"),
            "hashtags": row.get("hashtags"),
            "lang": row.get("lang"),
            "url": row.get("url"),
        }

        producer.send(KAFKA_TOPIC, key=event["id"], value=event)
        sent += 1

        if sent % batch_size == 0:
            producer.flush()
            print(f"Envoyé {sent} messages vers Kafka (topic={KAFKA_TOPIC})")
            time.sleep(sleep_seconds)

    producer.flush()
    print(f"Streaming terminé. Total messages envoyés : {sent}")


if __name__ == "__main__":
    stream_tweets()
