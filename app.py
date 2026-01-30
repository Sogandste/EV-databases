import os
import duckdb
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Config ---
APP_NAME = "EVisionary"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

con = duckdb.connect(database=':memory:')

# --- Helper: Remove .0 from Year ---
def clean_year(val):
    """Converts 2015.0 to 2015, and None to -"""
    if not val or str(val).lower() == "none" or str(val).lower() == "nan":
        return "-"
    s = str(val)
    # If it looks like a float (e.g. 2015.0), split it
    if "." in s:
        return s.split(".")[0]
    return s

# --- Strict Column Mapping ---
def map_columns_strictly(all_cols):
    """
    Finds the correct column names using strict lists to avoid mixing 'Vesicle Type' with 'Content Type'.
    """
    upper_cols = {c.upper(): c for c in all_cols}
    
    mapping = {}
    
    # 1. Molecule Type (Protein, mRNA, etc.)
    # We look for CONTENT_TYPE specifically first
    type_candidates = ["CONTENT_TYPE", "MOLECULE_TYPE", "BIOMOLECULE", "CAT_TYPE"]
    mapping['type'] = next((upper_cols[k] for k in type_candidates if k in upper_cols), None)

    # 2. Name / ID (Gene Symbol, Protein Name)
    name_candidates = ["GENE_SYMBOL", "GENE_NAME", "PROTEIN_NAME", "SYMBOL", "UNIPROT_ID", "NAME", "CONTENT_ID"]
    mapping['name'] = next((upper_cols[k] for k in name_candidates if k in upper_cols), None)

    # 3. Vesicle Type (sEV, Exosome) - MUST contain VESICLE or EV
    vesicle_candidates = ["VESICLE_TYPE", "EV_TYPE", "EXOSOME_TYPE", "SUBTYPE"]
    mapping['vesicle'] = next((upper_cols[k] for k in vesicle_candidates if k in upper_cols), None)

    # 4. Species
    species_candidates = ["SPECIES", "ORGANISM", "HOST_ORGANISM"]
    mapping['species'] = next((upper_cols[k] for k in species_candidates if k in upper_cols), "Species")

    # 5. Method
    method_candidates = ["ISOLATION_METHOD", "METHOD", "TECHNIQUE", "PURIFICATION"]
    mapping['method'] = next((upper_cols[k] for k in method_candidates if k in upper_cols), "Method")

    # 6. Year
    year_candidates = ["YEAR", "PUBLICATION_YEAR", "DATE"]
    mapping['year'] = next((upper_cols[k] for k in year_candidates if k in upper_cols), "Year")

    return mapping

# --- Initialize ---
def get_schema_info():
    try:
        schema_query = f"DESCRIBE SELECT * FROM '{DATA_PATH}' LIMIT 1"
        schema_df = con.execute(schema_query).fetchall()
        all_cols = [row[0] for row in schema_df]
        text_cols = [row[0] for row in schema_df if 'VARCHAR' in row[1] or 'STRING' in row[1]]
        
        # Use strict mapping
        col_map = map_columns_strictly(all_cols)
        print(f"✅ Final Column Mapping: {col_map}")
        
        return text_cols, col_map, len(all_cols)
    except Exception as e:
        print(f"❌ Schema Error: {e}")
        return [], {}, 0

SEARCHABLE_COLS, COL_MAP, TOTAL_COLS = get_schema_info()

# --- Search Logic ---
def search_duckdb(query, limit=100):
    try:
        if not SEARCHABLE_COLS: return []
        
        safe_query = query.replace("'", "''")
        where_clauses = [f"\"{col}\" ILIKE '%{safe_query}%'" for col in SEARCHABLE_COLS]
        full_where = " OR ".join(where_clauses)
        
        sql = f"SELECT * FROM '{DATA_PATH}' WHERE {full_where} LIMIT {limit}"
        result_rows = con.execute(sql).fetchall()
        
        desc = con.description
        res_col_names = [d[0] for d in desc]
        
        output = []
        for row in result_rows:
            row_dict = dict(zip(res_col_names, row))
            
            # --- Robust Data Extraction ---
            
            # 1. Molecule Type logic
            raw_type = row_dict.get(COL_MAP['type']) if COL_MAP['type'] else None
            # If explicit column missing, try to guess from the whole row, 
            # BUT DON'T put Vesicle Type here.
            mol_type = str(raw_type) if raw_type else "Unknown"
            if mol_type == "None" or mol_type == "Unknown":
                # Fallback guess only if strictly needed
                full_s = str(row_dict).lower()
                if "mirna" in full_s: mol_type = "miRNA"
                elif "protein" in full_s: mol_type = "Protein"
                elif "mrna" in full_s: mol_type = "mRNA"
            
            # 2. Name
            name_val = row_dict.get(COL_MAP['name'], "—")
            
            # 3. Vesicle Type
            vesicle_val = row_dict.get(COL_MAP['vesicle'], "—")

            output.append({
                "name": str(name_val),
                "type": mol_type,
                "species": str(row_dict.get(COL_MAP['species'], "—")),
                "vesicle": str(vesicle_val),
                "method": str(row_dict.get(COL_MAP['method'], "—")),
                "year": clean_year(row_dict.get(COL_MAP['year'])) # Clean the .0
            })
            
        return output

    except Exception as e:
        print(f"SQL Error: {e}")
        raise e

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html", app_name=APP_NAME)

@app.route("/stats")
def stats():
    try:
        count = con.execute(f"SELECT COUNT(*) FROM '{DATA_PATH}'").fetchone()[0]
    except:
        count = "700k+"
    return jsonify({"total_records": count})

@app.route("/search")
def search():
    query = request.args.get("q", "")
    limit = int(request.args.get("limit", 100))
    if not query: return jsonify([])
    try:
        return jsonify(search_duckdb(query, limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)