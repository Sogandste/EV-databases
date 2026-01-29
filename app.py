import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, Response
from ontology_terms import normalize_term

app = Flask(__name__)

# -------- Paths --------
LOCAL_PATH = "/Users/sogand/Downloads/EV_Databases/Unified_Output/unified_ev_metadata.parquet"
DOCKER_PATH = "/app/data/unified_ev_metadata.parquet"
DATA_PATH = DOCKER_PATH if os.path.exists(DOCKER_PATH) else LOCAL_PATH

# -------- Lazy data holder --------
_df = None

# فقط ستون‌هایی که UI لازم دارد
UI_COLUMNS = [
    "sample_type",
    "isolation_method",
    "species",
    "tissue",
    "vesicle_type",
    "database"
]

PAGE_SIZE = 25

def get_df():
    global _df
    if _df is None:
        _df = pd.read_parquet(DATA_PATH, columns=UI_COLUMNS)
    return _df

# -------- Home --------
@app.route("/")
def home():
    df = get_df()
    return render_template(
        "index.html",
        app_name="EVisionary",
        subtitle="Ontology‑aware harmonization and exploration of EV metadata",
        columns=list(df.columns)
    )

# -------- Search --------
@app.route("/search")
def search():
    df = get_df()

    col1 = request.args.get("column1", "")
    q1 = request.args.get("q1", "").strip()

    col2 = request.args.get("column2", "")
    q2 = request.args.get("q2", "").strip()

    operator = request.args.get("operator", "AND")
    ontology_term = request.args.get("ontology", "").strip()
    page = int(request.args.get("page", 1))

    filtered = df

    # ---- Vectorized filters ----
    if col1 in df.columns and q1:
        m1 = df[col1].astype(str).str.contains(q1, case=False, na=False)
    else:
        m1 = None

    if col2 in df.columns and q2:
        m2 = df[col2].astype(str).str.contains(q2, case=False, na=False)
    else:
        m2 = None

    if m1 is not None and m2 is not None:
        filtered = df[m1 & m2] if operator == "AND" else df[m1 | m2]
    elif m1 is not None:
        filtered = df[m1]
    elif m2 is not None:
        filtered = df[m2]

    # ---- Ontology-aware filter (SAFE) ----
    ont = normalize_term(ontology_term)
    if ont:
        label = ont["label"]
        text_blob = filtered.astype(str).agg(" ".join, axis=1)
        filtered = filtered[text_blob.str.contains(label, case=False, na=False)]

    # ---- Pagination ----
    total_pages = (len(filtered) - 1) // PAGE_SIZE + 1 if len(filtered) else 0
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE

    results = filtered.iloc[start:end].to_dict(orient="records")

    return render_template(
        "index.html",
        app_name="EVisionary",
        subtitle="Ontology‑aware harmonization and exploration of EV metadata",
        columns=list(df.columns),
        results=results,
        column1=col1,
        column2=col2,
        q1=q1,
        q2=q2,
        operator=operator,
        ontology=ontology_term,
        page=page,
        total_pages=total_pages
    )

# -------- Export --------
@app.route("/export")
def export_csv():
    df = get_df()
    ontology_term = request.args.get("ontology", "")
    ont = normalize_term(ontology_term)

    filtered = df
    if ont:
        label = ont["label"]
        blob = df.astype(str).agg(" ".join, axis=1)
        filtered = df[blob.str.contains(label, case=False, na=False)]

    return Response(
        filtered.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=evisionary_results.csv"}
    )

# -------- Case study --------
@app.route("/case-study/jev")
def case_study():
    df = get_df()
    return jsonify({
        "app": "EVisionary",
        "total_records": int(len(df)),
        "columns": list(df.columns)
    })

@app.route("/favicon.ico")
def favicon():
    return "", 204

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)