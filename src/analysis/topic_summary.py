from pymongo import MongoClient

MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"


TOPIC_LABELS = {
    0: "OTAN / UE / élargissement",
    1: "Politique US (Biden / Trump)",
    2: "Armement / puissance militaire",
    3: "Crise diplomatique / négociations",
    4: "Opinions générales sur Russie / USA",
    5: "Troupes aux frontières / invasion",
    6: "Russie–Chine / Taiwan / Syrie",
    7: "Risque nucléaire / OTAN / USA",
}


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db["tweets_topics"]

    pipeline = [
        {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    stats = list(coll.aggregate(pipeline))

    print("Répartition des topics (dominant_topic) :")
    total = sum(s["count"] for s in stats) or 1
    for s in stats:
        tid = s["_id"]
        c = s["count"]
        label = TOPIC_LABELS.get(tid, "Topic inconnu")
        pct = 100 * c / total
        print(f"Topic {tid} ({label}) : {c} tweets (~{pct:.1f}%)")

    client.close()


if __name__ == "__main__":
    main()
