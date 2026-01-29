import os
import pandas as pd
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# --------------------------------------------------
# App config
# --------------------------------------------------
APP_NAME = os.environ.get("APP_NAME", "EVisionary")

# --------------------------------------------------
# Data path (Render / Docker / Local safe)
# --------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

if not os.path.exists(DATA_PATH):
    raise RuntimeError(f"Parquet file not found at {DATA_PATH}")

# --------------------------------------------------
# Ontology-aware mappings (EV / GO – minimal but real)
# --------------------------------------------------
EV_ONTOLOGY = {
    "exosome": ["exosome", "small extracellular vesicle", "sev"],
    "microvesicle": ["microvesicle", "ectosome"],
    "extracellular vesicle": ["extracellular vesicle", "ev"]
}

GO_TERMS = {
    "extracellular region": ["GO:0005576"],
    "extracellular exosome": ["GO:0070062"],
    "extracellular vesicle": ["GO:1903561"]
}

# --------------------------------------------------
# Preferred UI columns (schema-safe)
# --------------------------------------------------
UI_COLUMNS = [
    "SAMPLE",
    "SAMPLE_SOURCE",
    "isolation_method",
    "species",
    "VESICLE_TYPE",
    "DATABASE"
]

_df = None


def get_df():
    """Lazy-load dataframe and select only existing columns."""
    global _df

    if _df is None:
        full_df = pd.read_parquet(DATA_PATH)
        safe_cols = [c for c in UI_COLUMNS if c in full_df.columns]
        _df = full_df[safe_cols]

    return _df


def ontology_filter(df, term):
    """
    Ontology-aware filtering for EV terms.
    Matches synonyms across VESICLE_TYPE column.
    """
    if "VESICLE_TYPE" not in df.columns:
        return df

    term = term.lower()

    synonyms = []
    if term in EV_ONTOLOGY:
        synonyms = EV_ONTOLOGY[term]
    else:
        synonyms = [term]

    mask = df["VESICLE_TYPE"].astype(str).str.lower().apply(
        lambda x: any(s in x for s in synonyms)
    )
    return df[mask]


# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    df = get_df()

    query = request.args.get("q", "").strip().lower()
    ev_term = request.args.get("ev_term", "").strip().lower()

    if ev_term:
        df = ontology_filter(df, ev_term)

    if query:
        mask = df.apply(
            lambda row: row.astype(str).str.lower().str.contains(query).any(),
            axis=1
        )
        df = df[mask]

    preview = df.head(200).to_dict(orient="records")

    return render_template(
        "index.html",
        app_name=APP_NAME,
        rows=preview,
        total=len(df),
        query=query,
        ev_term=ev_term
    )


# --------------------------------------------------
# ✅ Case Study Endpoint for JEV
# --------------------------------------------------
@app.route("/case-study/jev", methods=["GET"])
def case_study_jev():
    """
    Ontology-aware harmonization summary for JEV case study.
    Output is reviewer-ready and reproducible.
    """
    df = get_df()

    summary = {}

    if "VESICLE_TYPE" in df.columns:
        summary["vesicle_type_distribution"] = (
            df["VESICLE_TYPE"]
            .value_counts(dropna=True)
            .head(10)
            .to_dict()
        )

    if "isolation_method" in df.columns:
        summary["isolation_methods"] = (
            df["isolation_method"]
            .value_counts(dropna=True)
            .head(10)
            .to_dict()
        )

    if "species" in df.columns:
        summary["species_distribution"] = (
            df["species"]
            .value_counts(dropna=True)
            .to_dict()
        )

    summary["ontology"] = {
        "ev_terms": EV_ONTOLOGY,
        "go_terms": GO_TERMS
    }

    summary["metadata"] = {
        "total_records": len(df),
        "columns_used": list(df.columns),
        "application": APP_NAME
    }

    return jsonify(summary)


# --------------------------------------------------
# Run (Render-compatible)
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)