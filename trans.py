import os
import pandas as pd


# chemin du CSV brut (Twitter Ukraine war)
RAW_CSV_PATH = r"D:\3eme Année\projet big data\archive\Ukraine_war.csv"

# dossier de sortie dans le projet
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")
OUTPUT_PARQUET_PATH = os.path.join(OUTPUT_DIR, "ukraine_war_clean.parquet")

# colonnes à garder pour le projet
COLUMNS_TO_KEEP = [
	"id",
	"date",
	"content",
	"user",
	"replyCount",
	"retweetCount",
	"likeCount",
	"quoteCount",
	"conversationId",
	"inReplyToTweetId",
	"inReplyToUser",
	"mentionedUsers",
	"hashtags",
	"lang",
	"url",
]


def clean_ukraine_war_csv(
	csv_path: str = RAW_CSV_PATH,
	output_parquet: str = OUTPUT_PARQUET_PATH,
	chunksize: int = 200_000,
	lang_filter: str | None = "en",
) -> None:
	"""Nettoie le CSV Ukraine_war et écrit un Parquet propre dans data/.

	- garde uniquement les colonnes utiles
	- parse la date
	- filtre sur une langue (par défaut anglais)
	- enlève les lignes avec content vide ou très court
	- enlève les doublons sur id
	"""

	if not os.path.exists(csv_path):
		raise FileNotFoundError(f"Fichier CSV introuvable : {csv_path}")

	os.makedirs(os.path.dirname(output_parquet), exist_ok=True)

	print(f"Lecture et nettoyage depuis : {csv_path}")
	print(f"Écriture du Parquet nettoyé dans : {output_parquet}")

	cleaned_chunks: list[pd.DataFrame] = []
	total_rows = 0

	for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunksize)):
		print(f"\n--- Chunk {i} ---")
		total_rows += len(chunk)
		print(f"Lignes dans le chunk : {len(chunk)}")

		# garder uniquement les colonnes utiles qui existent vraiment dans le chunk
		cols_present = [c for c in COLUMNS_TO_KEEP if c in chunk.columns]
		df = chunk[cols_present].copy()

		# parser la date si présente
		if "date" in df.columns:
			df["date"] = pd.to_datetime(df["date"], errors="coerce")

		# filtrer sur la langue si demandé
		if lang_filter is not None and "lang" in df.columns:
			before = len(df)
			df = df[df["lang"] == lang_filter]
			print(f"  Filtre langue='{lang_filter}' : {before} -> {len(df)}")

		# enlever les content vides/trop courts
		if "content" in df.columns:
			before = len(df)
			df["content"] = df["content"].astype(str).str.strip()
			df = df[df["content"].str.len() > 5]
			print(f"  Filtre content longueur>5 : {before} -> {len(df)}")

		cleaned_chunks.append(df)

	if not cleaned_chunks:
		raise RuntimeError("Aucun chunk valide après nettoyage.")

	cleaned = pd.concat(cleaned_chunks, ignore_index=True)

	print("\n=========================")
	print("RÉSUMÉ APRÈS CONCATÉNATION")
	print("=========================")
	print(f"Total lignes lues          : {total_rows}")
	print(f"Total lignes après filtres : {len(cleaned)}")

	# enlever les doublons sur id si présent
	if "id" in cleaned.columns:
		before = len(cleaned)
		cleaned = cleaned.drop_duplicates(subset=["id"])
		print(f"Doublons sur id : {before} -> {len(cleaned)}")

	# sauvegarde en Parquet
	cleaned.to_parquet(output_parquet, index=False)
	print(f"\nDataset nettoyé sauvegardé dans : {output_parquet}")

	# petit aperçu
	print("\nAperçu des 10 premières lignes :")
	print(cleaned.head(10))


if __name__ == "__main__":
	clean_ukraine_war_csv()

