import re
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"
MAX_DOCS = 20000          # nombre max de tweets utilisés pour le modèle
N_TOPICS = 8              # nombre de topics
N_WORDS = 15              # nb de mots à afficher par topic


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r"http\S+|www\.\S+", " ", text)        # URLs
    text = re.sub(r"@[a-z0-9_]+", " ", text)             # mentions
    text = re.sub(r"#[a-z0-9_]+", " ", text)             # hashtags
    text = re.sub(r"[^a-z\s]", " ", text)                # garder lettres / espaces
    text = re.sub(r"\s+", " ", text).strip()
    return text


def load_texts():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db["tweets_clean"]

    cursor = coll.find(
        {"lang": "en", "content": {"$ne": None}},
        {"_id": 1, "content": 1},
        limit=MAX_DOCS,
    )

    ids = []
    texts = []
    for doc in cursor:
        ids.append(doc["_id"])
        texts.append(clean_text(doc.get("content", "")))

    client.close()
    return ids, texts


def build_lda(texts):
    vectorizer = CountVectorizer(
        max_df=0.9,
        min_df=10,
        max_features=5000,
        stop_words="english",
    )
    X = vectorizer.fit_transform(texts)

    lda = LatentDirichletAllocation(
        n_components=N_TOPICS,
        random_state=0,
        learning_method="batch",
    )
    lda.fit(X)
    return lda, X, vectorizer


def print_topics(lda, vectorizer):
    feature_names = vectorizer.get_feature_names_out()
    for k, topic in enumerate(lda.components_):
        top_indices = topic.argsort()[:-N_WORDS - 1:-1]
        top_words = [feature_names[i] for i in top_indices]
        print(f"\n=== Topic {k} ===")
        print(", ".join(top_words))


def save_topic_assignments(ids, lda, X):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db["tweets_topics"]

    topic_dist = lda.transform(X)          # matrice (nb_docs x N_TOPICS)
    docs = []
    for i, doc_id in enumerate(ids):
        dist = topic_dist[i]
        dominant = int(dist.argmax())
        docs.append(
            {
                "tweet_mongo_id": doc_id,
                "dominant_topic": dominant,
                "topic_distribution": dist.tolist(),
            }
        )

    coll.drop()
    if docs:
        coll.insert_many(docs)

    client.close()


def main():
    print("Chargement et nettoyage des tweets...")
    ids, texts = load_texts()
    print(f"{len(texts)} tweets chargés.")

    print("Construction du modèle LDA...")
    lda, X, vectorizer = build_lda(texts)

    print("\nMots principaux par topic :")
    print_topics(lda, vectorizer)

    print("\nSauvegarde des topics par tweet dans Mongo (collection 'tweets_topics')...")
    save_topic_assignments(ids, lda, X)
    print("Terminé.")


if __name__ == "__main__":
    main()
