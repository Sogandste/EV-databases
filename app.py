import os
import pandas as pd
from flask import Flask, render_template, request, jsonify, Response
from ontology_terms import normalize_term

app = Flask(__name__)

# ---------- Data path ----------
LOCAL_PATH = "/Users/sogand/Downloads/EV_Databases/Unified_Output/unified_ev_metadata.parquet"
DOCKER_PATH = "/app/data/unified_ev_metadata.parquet"
DATA_PATH = DOCKER_PATH if os.path.exists(DOCKER_PATH) else LOCAL_PATH

df = pd.read_parquet(DATA_PATH)
PAGE_SIZE = 25

# ---------- Home ----------
@app.route("/")
def home():
    return render_template("index.html", columns=list(df.columns))

# ---------- Search ----------
@app.route("/search")
def search():
    col1 = request.args.get("column1", "")
    q1 = request.args.get("q1", "").strip()

    col2 = request.args.get("column2", "")
    q2 = request.args.get("q2", "").strip()

    operator = request.args.get("operator", "AND")
    ontology_term = request.args.get("ontology", "").strip()
    page = int(request.args.get("page", 1))

    filtered = df

    # ---- Text filters ----
    mask1 = df[col1].astype(str).str.contains(q1, case=False, na=False) if col1 in df.columns and q1 else None
    mask2 = df[col2].astype(str).str.contains(q2, case=False, na=False) if col2 in df.columns and q2 else None

    if mask1 is not None and mask2 is not None:
        filtered = df[mask1 & mask2] if operator == "AND" else df[mask1 | mask2]
    elif mask1 is not None:
        filtered = df[mask1]
    elif mask2 is not None:
        filtered = df[mask2]

    # ---- Ontology-aware filter ----
    ont = normalize_term(ontology_term)
    if ont:
        term_label = ont["label"]
        filtered = filtered[
            filtered.apply(
                lambda row: term_label in " ".join(row.astype(str)).lower(),
                axis=1
            )
        ]

    # ---- Pagination ----
    total_pages = (len(filtered) - 1) // PAGE_SIZE + 1 if len(filtered) else 0
    start = (page - 1) * PAGE_SIZE
    end = start + PAGE_SIZE

    results = filtered.iloc[start:end].to_dict(orient="records")

    return render_template(
        "index.html",
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

# ---------- Export ----------
@app.route("/export")
def export_csv():
    ontology_term = request.args.get("ontology", "")
    ont = normalize_term(ontology_term)

    filtered = df
    if ont:
        filtered = filtered[
            filtered.apply(
                lambda row: ont["label"] in " ".join(row.astype(str)).lower(),
                axis=1
            )
        ]

    return Response(
        filtered.to_csv(index=False),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=ev_ontology_filtered.csv"}
    )

# ---------- Case study ----------
@app.route("/case-study/jev")
def case_study():
    return jsonify({
        "ontology_terms_supported": ["exosome", "extracellular vesicle", "microvesicle"],
        "total_records": int(len(df))
    })

@app.route("/favicon.ico")
def favicon():
    return "", 204

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)