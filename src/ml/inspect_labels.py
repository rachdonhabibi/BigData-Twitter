from pathlib import Path
import pandas as pd

BASE_DIR = Path("data")
FILES = ["twitter_training.csv", "twitter_validation.csv"]

for fname in FILES:
    path = BASE_DIR / fname
    print(f"\n=== {path} ===")
    df = pd.read_csv(path, header=None, names=["id", "topic", "sentiment", "text"])
    print(df["sentiment"].value_counts())
