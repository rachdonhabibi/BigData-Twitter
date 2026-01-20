from pymongo import MongoClient

MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"

TOP_COMMUNITIES = 5      # nombre de communautés à détailler
TOP_USERS_PER_COMM = 10  # top utilisateurs par communauté


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db["user_centrality"]

    # 1) Taille de chaque communauté
    pipeline = [
        {"$group": {"_id": "$community_id", "size": {"$sum": 1}}},
        {"$sort": {"size": -1}},
    ]
    communities = list(coll.aggregate(pipeline))

    print("=== Tailles des communautés (top) ===")
    for c in communities[:TOP_COMMUNITIES]:
        cid = c["_id"]
        size = c["size"]
        print(f"Community {cid}: size={size}")

    # 2) Pour les plus grosses communautés, top utilisateurs par degree
    for c in communities[:TOP_COMMUNITIES]:
        cid = c["_id"]
        size = c["size"]
        print(f"\n=== Community {cid} (size={size}) - Top {TOP_USERS_PER_COMM} by degree ===")
        top_users = (
            coll.find(
                {"community_id": cid},
                {
                    "_id": 0,
                    "username": 1,
                    "degree": 1,
                    "in_degree": 1,
                    "out_degree": 1,
                    "pagerank": 1,
                },
            )
            .sort("degree", -1)
            .limit(TOP_USERS_PER_COMM)
        )
        for i, u in enumerate(top_users, start=1):
            print(
                f"{i:2d}. {u['username']:<25} "
                f"deg={u.get('degree', 0):.1f}  "
                f"in={u.get('in_degree', 0):.1f}  "
                f"out={u.get('out_degree', 0):.1f}  "
                f"pr={u.get('pagerank', 0):.4f}"
            )

    client.close()


if __name__ == "__main__":
    main()
