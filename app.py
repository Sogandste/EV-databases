from flask import Flask, jsonify
import pandas as pd
import os

app = Flask(__name__)

# --------------------------------------------------
# Configuration: smart parquet path (local / Docker / Render)
# --------------------------------------------------

def get_parquet_path():
    """
    Detect parquet file path in different environments.
    Priority:
    1. ENV variable EV_PARQUET_PATH
    2. Local development path
    3. Docker/Render default path
    """
    if os.getenv("EV_PARQUET_PATH"):
        return os.getenv("EV_PARQUET_PATH")

    local_path = "/Users/sogand/Downloads/EV_Databases/Unified_Output/unified_ev_metadata.parquet"
    docker_path = "/app/data/unified_ev_metadata.parquet"

    if os.path.exists(local_path):
        return local_path
    elif os.path.exists(docker_path):
        return docker_path
    else:
        raise FileNotFoundError("Parquet file not found in known locations.")


def load_parquet():
    path = get_parquet_path()
    return pd.read_parquet(path)


# --------------------------------------------------
# Ontology & synonym mappings (minimal, defensible)
# --------------------------------------------------

EV_SYNONYMS = {
    "extracellular vesicle": [
        "extracellular vesicle",
        "extracellular vesicles",
        "ev",
        "evs",
        "exosome",
        "exosomes",
        "small extracellular vesicle",
        "sev",
        "sevs"
    ]
}

GO_TERMS = {
    "extracellular vesicle": "GO:1903561"
}


# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def filter_by_ev_synonyms(df, column_name="sample_description"):
    """
    Filter dataframe rows using EV-related synonyms.
    """
    terms = EV_SYNONYMS["extracellular vesicle"]
    pattern = "|".join(terms)

    if column_name not in df.columns:
        return df  # fail-safe: do not filter if column missing

    return df[df[column_name].str.contains(pattern, case=False, na=False)]


# --------------------------------------------------
# API endpoints
# --------------------------------------------------

@app.route("/")
def home():
    return jsonify({
        "message": "EV metadata harmonization API",
        "endpoints": ["/case-study/jev"]
    })


@app.route("/case-study/jev", methods=["GET"])
def case_study_jev():
    """
    JEV case study endpoint:
    Ontology-aware harmonization of EV proteomics metadata
    across EV-specific databases (ExoCarta & Vesiclepedia).
    """

    df = load_parquet()

    # ---- Filter EV-specific databases only ----
    if "source_database" in df.columns:
        df = df[df["source_database"].isin(["ExoCarta", "Vesiclepedia"])]

    # ---- Apply EV synonym harmonization ----
    df = filter_by_ev_synonyms(df)

    # ---- Build summary statistics ----
    summary = {
        "query": {
            "entity": "extracellular vesicle",
            "ontology": [GO_TERMS["extracellular vesicle"]],
            "synonyms_used": EV_SYNONYMS["extracellular vesicle"]
        },
        "results": {
            "total_records": int(len(df)),
            "unique_proteins": int(df["protein_accession"].nunique())
            if "protein_accession" in df.columns else None,
            "databases_covered": sorted(df["source_database"].unique().tolist())
            if "source_database" in df.columns else [],
            "species_distribution": df["species"].value_counts().to_dict()
            if "species" in df.columns else {}
        }
    }

    return jsonify(summary)


# --------------------------------------------------
# Run locally
# --------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)