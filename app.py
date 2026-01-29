import os
import pyarrow.parquet as pq
from collections import Counter
from flask import Flask, request, jsonify, render_template

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

YEAR_OPTIONS = list(range(2005, 2026))

SAFE_COLUMNS = ["VESICLE_TYPE", "species", "isolation_method", "YEAR"]

# --------------------------------------------------
# ✅ Streaming aggregation (NO pandas)
# --------------------------------------------------
def stream_summary(filters):
    vesicle_counter = Counter()
    total = 0

    for rg in range(_parquet.num_row_groups):
        table = _parquet.read_row_group(rg, columns=SAFE_COLUMNS)
        rows = table.to_pylist()  # row-by-row, released per group

        for r in rows:
            vt = str(r.get("VESICLE_TYPE", "")).lower()
            sp = r.get("species")
            iso = str(r.get("isolation_method", "")).lower()
            yr = r.get("YEAR")

            # EV filter
            if filters["ev_synonyms"]:
                if not any(s in vt for s in filters["ev_synonyms"]):
                    continue

            # species
            if filters["species"] and sp != filters["species"]:
                continue

            # isolation
            if filters["isolation"] and filters["isolation"] not in iso:
                continue

            # year
            if filters["year"] and yr != filters["year"]:
                continue

            total += 1
            vesicle_counter[vt] += 1

    return total, dict(vesicle_counter.most_common(10))


# --------------------------------------------------
# Routes
# --------------------------------------------------
@app.route("/", methods=["GET"])
def home():
    selected_ev = request.args.get("ev", "sEV (exosome-like)")
    selected_species = request.args.get("species", "")
    selected_isolation = request.args.get("isolation", "")
    selected_year = request.args.get("year", "")

    ontology = EV_ONTOLOGY.get(selected_ev)
    ev_syns = [s.lower() for s in ontology["synonyms"]] if ontology else []

    filters = {
        "ev_synonyms": ev_syns,
        "species": selected_species or None,
        "isolation": selected_isolation.lower() if selected_isolation else None,
        "year": int(selected_year) if selected_year.isdigit() else None
    }

    total, summary = stream_summary(filters)

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
        total_records=total,
        vesicle_summary=summary,
        go_term=ontology["go"] if ontology else "—"
    )


@app.route("/case-study/jev")
def case_study_jev():
    total, summary = stream_summary({
        "ev_synonyms": None,
        "species": None,
        "isolation": None,
        "year": None
    })

    return jsonify({
        "total_records": total,
        "vesicle_types": summary,
        "ontology": EV_ONTOLOGY,
        "application": APP_NAME,
        "deployment": "Render free tier",
        "memory_strategy": "row-group streaming aggregation"
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)