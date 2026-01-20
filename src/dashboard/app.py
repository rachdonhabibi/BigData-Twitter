import os
from datetime import datetime

import pandas as pd
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://root:password123@localhost:27017/?authSource=admin",
)
DB_NAME = "reddit_data"


@st.cache_data(show_spinner=False)
def load_collection(name: str) -> pd.DataFrame:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    coll = db[name]
    docs = list(coll.find({}, {"_id": 0}))
    client.close()
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


def dashboard_time():
    st.header("Dashboard 1 – Activité temporelle")

    df_daily = load_collection("tweets_kpi_daily")
    df_lang = load_collection("tweets_kpi_daily_lang")

    if df_daily.empty:
        st.warning("La collection 'tweets_kpi_daily' est vide ou introuvable.")
        return

    df_daily["date"] = pd.to_datetime(df_daily["date"])
    df_daily = df_daily.sort_values("date")

    if not df_lang.empty:
        df_lang["date"] = pd.to_datetime(df_lang["date"])
        df_lang = df_lang.sort_values("date")

    min_date = df_daily["date"].min()
    max_date = df_daily["date"].max()
    st.sidebar.subheader("Filtres – Dashboard 1")
    start, end = st.sidebar.date_input(
        "Période",
        value=(min_date.date(), max_date.date()),
        min_value=min_date.date(),
        max_value=max_date.date(),
    )

    mask = (df_daily["date"].dt.date >= start) & (df_daily["date"].dt.date <= end)
    df_daily_f = df_daily[mask]

    if not df_lang.empty:
        mask_lang = (df_lang["date"].dt.date >= start) & (df_lang["date"].dt.date <= end)
        df_lang_f = df_lang[mask_lang]
    else:
        df_lang_f = pd.DataFrame()

    total_tweets = int(df_daily_f["tweets_count"].sum())
    total_eng = int(df_daily_f["engagement_sum"].sum())
    avg_eng = total_eng / total_tweets if total_tweets > 0 else 0
    nb_days = df_daily_f["date"].nunique()

    if not df_daily_f.empty:
        idx_max = df_daily_f["tweets_count"].idxmax()
        max_tweets = int(df_daily_f.loc[idx_max, "tweets_count"])
        date_peak = df_daily_f.loc[idx_max, "date"].date()
    else:
        max_tweets = 0
        date_peak = "-"

    st.subheader("KPI globaux")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total de tweets", f"{total_tweets:,}".replace(",", " "))
    c2.metric("Engagement total", f"{total_eng:,}".replace(",", " "))
    c3.metric("Engagement moyen / tweet", f"{avg_eng:.2f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Nombre de jours couverts", nb_days)
    c5.metric("Max de tweets en un jour", f"{max_tweets:,}".replace(",", " "))
    c6.metric("Date du pic de tweets", str(date_peak))

    st.subheader("Tweets par jour")
    st.line_chart(
        df_daily_f.set_index("date")[["tweets_count"]],
        y_label="Tweets / jour",
    )

    st.subheader("Engagement par jour")
    st.line_chart(
        df_daily_f.set_index("date")[["engagement_sum"]],
        y_label="Engagement / jour",
    )

    if not df_lang_f.empty:
        st.subheader("Tweets par jour et par langue")
        pivot = df_lang_f.pivot_table(
            index="date", columns="lang", values="tweets_count", aggfunc="sum"
        ).fillna(0)
        st.area_chart(pivot, y_label="Tweets / jour")


def dashboard_topics_users():
    st.header("Dashboard 2 – Thématiques & acteurs")

    df_hashtags = load_collection("tweets_kpi_hashtags")
    df_users = load_collection("tweets_kpi_users")

    if df_hashtags.empty or df_users.empty:
        st.warning(
            "Les collections 'tweets_kpi_hashtags' ou 'tweets_kpi_users' sont vides ou introuvables."
        )
        return

    st.sidebar.subheader("Filtres – Dashboard 2")
    top_n = st.sidebar.slider("Top N à afficher", min_value=5, max_value=50, value=20)

    nb_hashtags = int(df_hashtags["hashtag"].nunique())
    tweets_with_hashtag = int(df_hashtags["tweets_count"].sum())

    idx_top_hashtag = df_hashtags["tweets_count"].idxmax()
    top_hashtag = df_hashtags.loc[idx_top_hashtag, "hashtag"]
    top_hashtag_tweets = int(df_hashtags.loc[idx_top_hashtag, "tweets_count"])

    idx_top_user = df_users["engagement_sum"].idxmax()
    top_user = df_users.loc[idx_top_user, "username"]
    top_user_eng = int(df_users.loc[idx_top_user, "engagement_sum"])

    st.subheader("KPI hashtags & utilisateurs")
    c1, c2, c3 = st.columns(3)
    c1.metric("Nombre de hashtags distincts", nb_hashtags)
    c2.metric("Tweets avec au moins un hashtag", f"{tweets_with_hashtag:,}".replace(",", " "))
    c3.metric("Hashtag le plus utilisé", f"#{top_hashtag}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Tweets du hashtag le plus utilisé", f"{top_hashtag_tweets:,}".replace(",", " "))
    c5.metric("Utilisateur top engagement", top_user)
    c6.metric(
        "Engagement total du top utilisateur",
        f"{top_user_eng:,}".replace(",", " "),
    )

    st.subheader(f"Top {top_n} hashtags par nombre de tweets")
    top_hashtags_count = (
        df_hashtags.sort_values("tweets_count", ascending=False)
        .head(top_n)
        .set_index("hashtag")[["tweets_count"]]
    )
    st.bar_chart(top_hashtags_count)

    st.subheader(f"Top {top_n} hashtags par engagement")
    top_hashtags_eng = (
        df_hashtags.sort_values("engagement_sum", ascending=False)
        .head(top_n)
        .set_index("hashtag")[["engagement_sum"]]
    )
    st.bar_chart(top_hashtags_eng)

    st.subheader(f"Top {top_n} utilisateurs par engagement")
    top_users_eng = (
        df_users.sort_values("engagement_sum", ascending=False)
        .head(top_n)
        .set_index("username")[["engagement_sum"]]
    )
    st.bar_chart(top_users_eng)

    st.subheader("Table détaillée des utilisateurs (Top 100)")
    st.dataframe(
        df_users.sort_values("engagement_sum", ascending=False)
        .head(100)
        .reset_index(drop=True)
    )


def main():
    st.set_page_config(page_title="Twitter Ukraine – Dashboards", layout="wide")
    st.title("Projet Big Data – Twitter Ukraine")

    tab1, tab2 = st.tabs(["1. Temps & langues", "2. Thématiques & acteurs"])

    with tab1:
        dashboard_time()
    with tab2:
        dashboard_topics_users()


if __name__ == "__main__":
    main()