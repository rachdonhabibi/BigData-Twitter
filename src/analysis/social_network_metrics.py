from pymongo import MongoClient
import networkx as nx


MONGO_URI = "mongodb://root:password123@localhost:27017/?authSource=admin"
DB_NAME = "reddit_data"


def load_graph():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    edges_col = db["user_edges"]
    nodes_col = db["user_nodes"]

    # Charger les noeuds
    G = nx.DiGraph()
    for n in nodes_col.find({}, {"username": 1, "_id": 0}):
        G.add_node(n["username"])

    # Charger les arêtes
    for e in edges_col.find({}, {"source_user": 1, "target_user": 1, "weight": 1, "_id": 0}):
        src = e["source_user"]
        tgt = e["target_user"]
        w = e.get("weight", 1)
        if G.has_edge(src, tgt):
            G[src][tgt]["weight"] += w
        else:
            G.add_edge(src, tgt, weight=w)

    return G, client


def compute_centrality_and_communities(G, client):
    db = client[DB_NAME]

    # 1) Centralités de base
    in_deg = dict(G.in_degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight"))
    deg = dict(G.degree(weight="weight"))

    # 2) PageRank (sur le graphe dirigé)
    try:
        pr = nx.pagerank(G, weight="weight")
    except Exception:
        pr = {n: 0.0 for n in G.nodes()}

    # 3) Communautés (sur version non orientée)
    G_undirected = G.to_undirected()
    from networkx.algorithms.community import label_propagation_communities

    communities = list(label_propagation_communities(G_undirected))
    user_to_community = {}
    for cid, comm in enumerate(communities):
        for u in comm:
            user_to_community[u] = cid

    # 4) Construire et enregistrer les documents
    user_centrality_docs = []
    for u in G.nodes():
        doc = {
            "username": u,
            "in_degree": float(in_deg.get(u, 0)),
            "out_degree": float(out_deg.get(u, 0)),
            "degree": float(deg.get(u, 0)),
            "pagerank": float(pr.get(u, 0.0)),
            "community_id": int(user_to_community.get(u, -1)),
        }
        user_centrality_docs.append(doc)

    # Remplacer les collections
    db["user_centrality"].drop()
    if user_centrality_docs:
        db["user_centrality"].insert_many(user_centrality_docs)


def main():
    G, client = load_graph()
    compute_centrality_and_communities(G, client)
    client.close()


if __name__ == "__main__":
    main()
