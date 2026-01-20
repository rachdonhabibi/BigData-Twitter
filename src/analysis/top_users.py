from pymongo import MongoClient
import csv
from pathlib import Path

MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"

TOP_N = 20


def get_top_users(db, field):
    coll = db["user_centrality"]
    cursor = (
        coll.find(
            {},
            {
                "_id": 0,
                "username": 1,
                "degree": 1,
                "in_degree": 1,
                "out_degree": 1,
                "pagerank": 1,
                "community_id": 1,
            },
        )
        .sort(field, -1)
        .limit(TOP_N)
    )
    return list(cursor)


def save_csv(rows, filepath):
    if not rows:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "username",
                "degree",
                "in_degree",
                "out_degree",
                "pagerank",
                "community_id",
            ],
        )
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    out_dir = Path("outputs/top_users")
    metrics = ["degree", "in_degree", "pagerank"]

    for m in metrics:
        print(f"\n=== Top {TOP_N} users by {m} ===")
        top = get_top_users(db, m)
        for i, u in enumerate(top, start=1):
            print(
                f"{i:2d}. {u['username']:<20} "
                f"{m}={u.get(m, 0):.4f}  "
                f"deg={u.get('degree', 0):.1f}  "
                f"in={u.get('in_degree', 0):.1f}  "
                f"out={u.get('out_degree', 0):.1f}  "
                f"comm={u.get('community_id', -1)}"
            )
        save_csv(top, out_dir / f"top_{m}.csv")

    client.close()


if __name__ == "__main__":
    main()

