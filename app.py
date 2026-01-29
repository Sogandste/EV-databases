import os
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask, request, jsonify

app = Flask(__name__)

# --------------------------------------------------
# Config
# --------------------------------------------------
APP_NAME = os.environ.get("APP_NAME", "EVisionary")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

if not os.path.exists(DATA_PATH):
    raise RuntimeError(f"Parquet file not found: {DATA_PATH}")

# --------------------------------------------------
# Ontology mappings (minimal, real)
# --------------------------------------------------
EV_ONTOLOGY = {
    "exosome": ["exosome", "small extracellular vesicle", "sev"],
    "microvesicle": ["microvesicle", "ectosome"],
    "extracellular vesicle": ["extracellular vesicle", "ev"]
}

GO_TERMS = {
    "extracellular region": "GO:0005576",
    "extracellular exosome": "GO:0070062",
    "extracellular vesicle": "GO:1903561"
}

# --------------------------------------------------
# Columns we actually need (verified from schema)
# --------------------------------------------------
SAFE_COLUMNS = [
    "VESICLE_TYPE",
    "isolation_method",
    "species",
    "DATABASE"
]

# --------------------------------------------------
# Parquet reader (memory-safe)
# --------------------------------------------------
_parquet = pq.ParquetFile(DATA_PATH)


def read_safe_df(columns):
    """
    Read only selected columns using low-level pyarrow API
    to avoid loading the full dataset into memory.
    """
    table = _parquet.read(columns=columns)
    return table.to_pandas()


# --------------------------------------------------
# Ontology filter
# --------------------------------------------------
def filter_by_ev_ontology(df, term):
    if "VESICLE_TYPE" not in df.columns:
        return df

    term = term.lower()
    synonyms = EV_ONTOLOGY.get(term, [term])

    mask = df["VESICLE_TYPE"].astype(str).str.lower().apply(
        lambda x: any(s in x for s in synonyms)
    )

    return df[mask]


# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    ev_term = request.args.get("ev", "").strip().lower()

    df = read_safe_df(SAFE_COLUMNS)

    if ev_term:
        df = filter_by_ev_ontology(df, ev_term)

    return jsonify({
        "status": "ok",
        "records": len(df),
        "columns": list(df.columns),
        "ev_filter": ev_term or None
    })


# --------------------------------------------------
# ✅ Case Study Endpoint – JEV (OOM-safe)
# --------------------------------------------------
@app.route("/case-study/jev", methods=["GET"])
def case_study_jev():
    df = read_safe_df(SAFE_COLUMNS)

    summary = {
        "total_records": len(df),
        "vesicle_types": (
            df["VESICLE_TYPE"].value_counts().head(10).to_dict()
            if "VESICLE_TYPE" in df.columns else {}
        ),
        "isolation_methods": (
            df["isolation_method"].value_counts().head(10).to_dict()
            if "isolation_method" in df.columns else {}
        ),
        "species": (
            df["species"].value_counts().to_dict()
            if "species" in df.columns else {}
        ),
        "ontology": {
            "ev_terms": EV_ONTOLOGY,
            "go_terms": GO_TERMS
        },
        "reproducibility": {
            "data_file": "unified_ev_metadata.parquet",
            "deployment": "Render free tier",
            "application": APP_NAME
        }
    }

    return jsonify(summary)


# --------------------------------------------------
# Run
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)