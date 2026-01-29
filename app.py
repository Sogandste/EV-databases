import os
import pandas as pd
from flask import Flask, request, jsonify, render_template

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
# Ontology-aware mappings (minimal, real)
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
# Lazy-loaded dataframe
# --------------------------------------------------
_df = None


def get_df():
    """
    Absolutely schema-safe loader.
    No column projection at read time (prevents ArrowInvalid).
    """
    global _df

    if _df is None:
        _df = pd.read_parquet(DATA_PATH)

    return _df


# --------------------------------------------------
# Ontology-aware EV filter
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
    df = get_df()

    query = request.args.get("q", "").strip().lower()
    ev_term = request.args.get("ev", "").strip().lower()

    if ev_term:
        df = filter_by_ev_ontology(df, ev_term)

    if query:
        mask = df.apply(
            lambda row: row.astype(str).str.lower().str.contains(query).any(),
            axis=1
        )
        df = df[mask]

    # ultra-safe response (template-independent)
    return jsonify({
        "status": "ok",
        "application": APP_NAME,
        "records": len(df),
        "columns": list(df.columns),
        "ev_filter": ev_term or None
    })


# --------------------------------------------------
# ✅ Case Study Endpoint – JEV
# --------------------------------------------------
@app.route("/case-study/jev", methods=["GET"])
def case_study_jev():
    df = get_df()

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
            "deployment": "Render",
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