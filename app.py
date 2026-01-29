import os
import pyarrow.parquet as pq
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --------------------------------------------------
# Config
# --------------------------------------------------
APP_NAME = os.environ.get("APP_NAME", "EVisionary")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

if not os.path.exists(DATA_PATH):
    raise RuntimeError("Parquet file not found")

_parquet = pq.ParquetFile(DATA_PATH)

# --------------------------------------------------
# Ontology-aware EV definitions (MISEV-aligned)
# --------------------------------------------------
EV_ONTOLOGY = {
    "sEV (exosome-like)": {
        "synonyms": ["exosome", "small extracellular vesicle", "sev"],
        "go": "GO:0070062"
    },
    "microvesicle": {
        "synonyms": ["microvesicle", "ectosome"],
        "go": "GO:1903561"
    },
    "extracellular vesicle (generic)": {
        "synonyms": ["extracellular vesicle", "ev"],
        "go": "GO:1903561"
    },
    "apoptotic body": {
        "synonyms": ["apoptotic body"],
        "go": "GO:0097209"
    }
}

# --------------------------------------------------
# Controlled vocabularies (RAM-safe)
# --------------------------------------------------
SPECIES_OPTIONS = [
    "Homo sapiens",
    "Mus musculus",
    "Rattus norvegicus",
    "Bos taurus",
    "Danio rerio"
]

ISOLATION_OPTIONS = [
    "ultracentrifugation",
    "size exclusion chromatography",
    "precipitation",
    "density gradient",
    "immunoaffinity"
]

# ✅ YEAR range (static, safe, no scan)
YEAR_OPTIONS = list(range(2005, 2026))

SAFE_COLUMNS = [
    "VESICLE_TYPE",
    "species",
    "isolation_method",
    "YEAR"
]

# --------------------------------------------------
# Memory-safe parquet reader
# --------------------------------------------------
def read_df():
    return _parquet.read(columns=SAFE_COLUMNS).to_pandas()


# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    selected_ev = request.args.get("ev", "sEV (exosome-like)")
    selected_species = request.args.get("species", "")
    selected_isolation = request.args.get("isolation", "")
    selected_year = request.args.get("year", "")

    df = read_df()

    # EV ontology filter
    ontology = EV_ONTOLOGY.get(selected_ev)
    if ontology:
        syns = ontology["synonyms"]
        df = df[
            df["VESICLE_TYPE"]
            .astype(str)
            .str.lower()
            .apply(lambda x: any(s in x for s in syns))
        ]

    # Species filter
    if selected_species:
        df = df[df["species"] == selected_species]

    # Isolation filter
    if selected_isolation:
        df = df[
            df["isolation_method"]
            .astype(str)
            .str.lower()
            .str.contains(selected_isolation.lower())
        ]

    # ✅ YEAR filter (null-safe)
    if selected_year:
        try:
            year_int = int(selected_year)
            df = df[df["YEAR"] == year_int]
        except ValueError:
            pass  # ignore invalid year silently

    summary = df["VESICLE_TYPE"].value_counts().head(10).to_dict()

    return render_template(
        "index.html",
        app_name=APP_NAME,
        ev_options=list(EV_ONTOLOGY.keys()),
        species_options=SPECIES_OPTIONS,
        isolation_options=ISOLATION_OPTIONS,
        year_options=YEAR_OPTIONS,
        selected_ev=selected_ev,
        selected_species=selected_species,
        selected_isolation=selected_isolation,
        selected_year=selected_year,
        total_records=len(df),
        vesicle_summary=summary,
        go_term=ontology["go"] if ontology else "—"
    )


@app.route("/case-study/jev", methods=["GET"])
def case_study_jev():
    df = read_df()

    return jsonify({
        "total_records": len(df),
        "vesicle_types": df["VESICLE_TYPE"].value_counts().to_dict(),
        "species_distribution": df["species"].value_counts().to_dict(),
        "isolation_methods": df["isolation_method"].value_counts().to_dict(),
        "year_distribution": df["YEAR"].value_counts().sort_index().to_dict(),
        "ontology": EV_ONTOLOGY,
        "application": APP_NAME,
        "deployment": "Render free tier"
    })


# --------------------------------------------------
# Run
# --------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)