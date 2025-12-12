import pandas as pd
from pathlib import Path
import sqlite3

# etl_spotify.py est dans ./dags
BASE_DIR = Path(__file__).resolve().parent.parent
# Sur ta machine : /Users/.../opensound
# Dans le conteneur : /opt/airflow

RAW_PATH = BASE_DIR / "data" / "raw" / "spotify_tracks_raw.csv"
DB_DIR = BASE_DIR / "data" / "processed"
DB_PATH = DB_DIR / "opensound.db"
TABLE_NAME = "spotify_tracks"


def run_etl():
    print(f"[EXTRACT] Lecture du fichier brut : {RAW_PATH}")
    df = pd.read_csv(RAW_PATH)
    print(f"[EXTRACT] {len(df):,} lignes chargées, {len(df.columns)} colonnes")

    cols_to_keep = [
        "track_id",
        "track_name",
        "artists",
        "album_name",
        "track_genre",
        "popularity",
        "explicit",
        "duration_ms",
        "danceability",
        "energy",
        "valence",
        "tempo",
    ]
    missing = [c for c in cols_to_keep if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le CSV : {missing}")

    df = df[cols_to_keep]

    for col in ["track_name", "artists", "album_name", "track_genre"]:
        df[col] = df[col].astype(str).str.strip()

    num_cols = ["popularity", "duration_ms", "danceability", "energy", "valence", "tempo"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["track_name", "artists"])

    df["duration_min"] = (df["duration_ms"] / 1000) / 60

    def popularity_bucket(x):
        if pd.isna(x):
            return "Inconnu"
        x = int(x)
        if x < 20:
            return "0–19 (très faible)"
        elif x < 40:
            return "20–39 (faible)"
        elif x < 60:
            return "40–59 (moyenne)"
        elif x < 80:
            return "60–79 (élevée)"
        else:
            return "80–100 (très élevée)"

    df["popularity_bucket"] = df["popularity"].apply(popularity_bucket)
    df["track_genre"] = df["track_genre"].str.lower()

    DB_DIR.mkdir(parents=True, exist_ok=True)
    print(f"[LOAD] Connexion à SQLite : {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        count = cur.fetchone()[0]
        print(f"[LOAD] Table '{TABLE_NAME}' chargée avec {count:,} lignes.")


if __name__ == "__main__":
    run_etl()
