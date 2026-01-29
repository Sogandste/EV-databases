import os
import csv
import io
import pyarrow.parquet as pq
from flask import Flask, request, jsonify, render_template, Response

app = Flask(__name__)

# --------------------------------------------------
# Config
# --------------------------------------------------
APP_NAME = os.environ.get("APP_NAME", "EVisionary")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

_parquet = pq.ParquetFile(DATA_PATH)

# --------------------------------------------------
# Ontology-aware EV definitions
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

SPECIES_OPTIONS = [
    "Homo sapiens", "Mus musculus", "Rattus norvegicus",
    "Bos taurus", "Danio rerio"
]

ISOLATION_OPTIONS = [
    "ultracentrifugation",
    "size exclusion chromatography",
    "precipitation",
    "density gradient",
    "immunoaffinity"
]

YEAR_OPTIONS = list(range(2005, 2026))

SAFE_COLUMNS = ["VESICLE_TYPE", "species", "isolation_method", "YEAR"]


# --------------------------------------------------
# Helper: streaming row filter
# --------------------------------------------------
def row_matches(r, filters, search=None):
    vt = str(r["VESICLE_TYPE"]).lower()

    if filters["ev_syns"]:
        if not any(s in vt for s in filters["ev_syns"]):
            return False

    if filters["species"] and r["species"] != filters["species"]:
        return False

    if filters["isolation"] and filters["isolation"] not in str(r["isolation_method"]).lower():
        return False

    if filters["year"] and r["YEAR"] != filters["year"]:
        return False

    if search and search not in vt:
        return False

    return True


# --------------------------------------------------
# UI
# --------------------------------------------------
@app.route("/")
def home():
    ev = request.args.get("ev", "sEV (exosome-like)")
    ontology = EV_ONTOLOGY.get(ev)

    return render_template(
        "index.html",
        app_name=APP_NAME,
        ev_options=list(EV_ONTOLOGY.keys()),
        species_options=SPECIES_OPTIONS,
        isolation_options=ISOLATION_OPTIONS,
        year_options=YEAR_OPTIONS,
        selected_ev=ev,
        go_term=ontology["go"] if ontology else "—"
    )


# --------------------------------------------------
# DataTables API (server-side, RAM-safe)
# --------------------------------------------------
@app.route("/api/table")
def api_table():
    start = int(request.args.get("start", 0))
    length = int(request.args.get("length", 25))
    search = request.args.get("search[value]", "").lower()

    filters = {
        "ev_syns": [
            s.lower()
            for s in EV_ONTOLOGY.get(
                request.args.get("ev", ""), {}
            ).get("synonyms", [])
        ],
        "species": request.args.get("species") or None,
        "isolation": request.args.get("isolation", "").lower() or None,
        "year": int(request.args.get("year"))
        if request.args.get("year", "").isdigit()
        else None
    }

    rows = []
    matched = 0

    for rg in range(_parquet.num_row_groups):
        table = _parquet.read_row_group(rg, columns=SAFE_COLUMNS).to_pylist()

        for r in table:
            if not row_matches(r, filters, search):
                continue

            matched += 1

            if matched <= start:
                continue
            if len(rows) >= length:
                break

            rows.append([
                r["VESICLE_TYPE"],
                r["species"],
                r["isolation_method"],
                r["YEAR"]
            ])

        if len(rows) >= length:
            break

    return jsonify({
        "draw": int(request.args.get("draw", 1)),
        "recordsTotal": matched,
        "recordsFiltered": matched,
        "data": rows
    })


# --------------------------------------------------
# ✅ CSV export – current page only
# --------------------------------------------------
@app.route("/api/export")
def export_csv():
    filters = {
        "ev_syns": [
            s.lower()
            for s in EV_ONTOLOGY.get(
                request.args.get("ev", ""), {}
            ).get("synonyms", [])
        ],
        "species": request.args.get("species") or None,
        "isolation": request.args.get("isolation", "").lower() or None,
        "year": int(request.args.get("year"))
        if request.args.get("year", "").isdigit()
        else None
    }

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Vesicle type", "Species", "Isolation", "Year"])

    count = 0
    for rg in range(_parquet.num_row_groups):
        table = _parquet.read_row_group(rg, columns=SAFE_COLUMNS).to_pylist()
        for r in table:
            if not row_matches(r, filters):
                continue
            writer.writerow([
                r["VESICLE_TYPE"],
                r["species"],
                r["isolation_method"],
                r["YEAR"]
            ])
            count += 1
            if count >= 1000:  # hard safety cap
                break
        if count >= 1000:
            break

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": "attachment; filename=ev_query_page.csv"
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)