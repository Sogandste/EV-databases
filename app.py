import os
import duckdb
from flask import Flask, request, jsonify, render_template

from synonyms import SYNONYM_MAP
from ontology_terms import ONTOLOGY_MAP

app = Flask(__name__, template_folder="templates")

# ----------------------------------
# Path handling (local vs Render)
# ----------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

PARQUET_PATH = os.environ.get(
    "EV_PARQUET_PATH",
    os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")
)

# ----------------------------------
# Load data once
# ----------------------------------
con = duckdb.connect(database=":memory:")
con.execute(f"""
    CREATE TABLE ev_metadata AS
    SELECT * FROM read_parquet('{PARQUET_PATH}')
""")

# ----------------------------------
# Normalization (synonym + ontology)
# ----------------------------------
def normalize_term(term: str) -> str:
    t = term.lower().strip()

    if t in SYNONYM_MAP:
        return SYNONYM_MAP[t]

    if t in ONTOLOGY_MAP:
        return ONTOLOGY_MAP[t]["label"]

    return t


# ----------------------------------
# UI
# ----------------------------------
@app.route("/")
def index():
    return render_template("index.html")


# ----------------------------------
# Health check (Render)
# ----------------------------------
@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


# ----------------------------------
# Search API
# ----------------------------------
@app.route("/api/search")
def search():
    raw_term = request.args.get("term")
    if not raw_term:
        return jsonify({"error": "term parameter is required"}), 400

    term = normalize_term(raw_term)

    query = f"""
        SELECT
            study_accession,
            sample_id,
            tissue,
            isolation_method,
            disease
        FROM ev_metadata
        WHERE
            LOWER(tissue) LIKE '%{term}%'
            OR LOWER(isolation_method) LIKE '%{term}%'
            OR LOWER(disease) LIKE '%{term}%'
        LIMIT 100
    """

    df = con.execute(query).df()
    return jsonify(df.to_dict(orient="records"))


# ----------------------------------
# Case‑study endpoint
# ----------------------------------
@app.route("/api/case-study/msc-ev")
def case_study():
    query = """
        SELECT
            study_accession,
            sample_id,
            tissue,
            isolation_method,
            disease
        FROM ev_metadata
        WHERE
            LOWER(tissue) LIKE '%mesenchymal%'
            AND LOWER(isolation_method) LIKE '%ultracentrifugation%'
    """

    df = con.execute(query).df()
    return jsonify({
        "case_study": "MSC-derived EVs",
        "n_samples": len(df),
        "results": df.to_dict(orient="records")
    })


# ----------------------------------
# Entrypoint (Render-safe)
# ----------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)