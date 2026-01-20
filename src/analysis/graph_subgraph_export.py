from pymongo import MongoClient
import networkx as nx
from pathlib import Path

MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"

TOP_N_NODES = 100  # nb d'utilisateurs à garder dans le sous-graphe


def load_top_nodes(client):
    db = client[DB_NAME]
    coll = db["user_centrality"]

    # Top N par degree
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
        .sort("degree", -1)
        .limit(TOP_N_NODES)
    )
    top_users = list(cursor)
    top_usernames = {u["username"] for u in top_users}
    return top_users, top_usernames


def build_subgraph(client, top_usernames, top_users):
    db = client[DB_NAME]
    edges_coll = db["user_edges"]

    G = nx.DiGraph()

    # Ajouter les noeuds avec leurs attributs
    attrs = {}
    for u in top_users:
        uname = u["username"]
        G.add_node(uname)
        attrs[uname] = {
            "degree": float(u.get("degree", 0)),
            "in_degree": float(u.get("in_degree", 0)),
            "out_degree": float(u.get("out_degree", 0)),
            "pagerank": float(u.get("pagerank", 0)),
            "community_id": int(u.get("community_id", -1)),
        }
    nx.set_node_attributes(G, attrs)

    # Ajouter les arêtes entre ces noeuds seulement
    cursor = edges_coll.find(
        {
            "source_user": {"$in": list(top_usernames)},
            "target_user": {"$in": list(top_usernames)},
        },
        {"_id": 0, "source_user": 1, "target_user": 1, "weight": 1, "type": 1},
    )

    for e in cursor:
        src = e["source_user"]
        tgt = e["target_user"]
        w = float(e.get("weight", 1))
        etype = e.get("type", "unknown")
        if G.has_edge(src, tgt):
            G[src][tgt]["weight"] += w
        else:
            G.add_edge(src, tgt, weight=w, type=etype)

    return G


def main():
    client = MongoClient(MONGO_URI)

    top_users, top_usernames = load_top_nodes(client)
    G = build_subgraph(client, top_usernames, top_users)

    out_dir = Path("outputs/graphs")
    out_dir.mkdir(parents=True, exist_ok=True)
    gexf_path = out_dir / "ukraine_top100_subgraph.gexf"

    nx.write_gexf(G, gexf_path.as_posix())
    print(f"Sous-graphe ({len(G.nodes())} noeuds, {len(G.edges())} arêtes) exporté vers : {gexf_path}")

    client.close()


if __name__ == "__main__":
    main()
