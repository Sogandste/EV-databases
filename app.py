import os
import duckdb
from flask import Flask, jsonify, request, render_template
import re

app = Flask(__name__)

# --- Config ---
APP_NAME = "EVisionary"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

con = duckdb.connect(database=':memory:')

# --- 1. CLEANING FUNCTIONS (The Makeover) ---
def clean_text(val):
    """
    Cleans dirty string data:
    - Removes underscores
    - Handles None/NaN
    - Title cases text (e.g. 'homo_sapiens' -> 'Homo Sapiens')
    """
    if val is None: return "—"
    s = str(val).strip()
    
    # Check for empty/null strings
    if s.lower() in ["none", "nan", "null", "", "n/a"]:
        return "—"
    
    # Remove underscores
    s = s.replace("_", " ")
    
    # Fix spacing issues
    s = " ".join(s.split())
    
    return s

def standardize_mol_type(val, full_row_str):
    """
    Forces standard scientific casing:
    - mrna, MRNA -> mRNA
    - mirna -> miRNA
    - protein -> Protein
    """
    # First, look at the specific value
    s = str(val).lower() if val else ""
    
    # If specific value is empty, scan the whole row (smart guess)
    if s in ["none", "nan", "", "—"]:
        s = full_row_str.lower()
    
    # Logic priority
    if "mirna" in s: return "miRNA"
    if "mrna" in s: return "mRNA"
    if "protein" in s or "uniprot" in s: return "Protein"
    if "lipid" in s: return "Lipid"
    
    return "Other" # Default if nothing matches

def clean_year(val):
    if not val: return "—"
    s = str(val)
    if "." in s: s = s.split(".")[0] # Remove .0
    if s.lower() in ["none", "nan"]: return "—"
    return s

# --- 2. COLUMN MAPPING ---
def get_schema_info():
    try:
        # Check available columns
        df = con.execute(f"DESCRIBE SELECT * FROM '{DATA_PATH}' LIMIT 1").fetchall()
        all_cols = [r[0] for r in df]
        text_cols = [r[0] for r in df if 'VARCHAR' in r[1] or 'STRING' in r[1]]
        
        upper_map = {c.upper(): c for c in all_cols}
        
        # Priority mapping
        col_map = {
            "name": next((upper_map[k] for k in ["GENE_SYMBOL", "GENE_NAME", "PROTEIN_NAME", "NAME", "SYMBOL", "CONTENT_ID"] if k in upper_map), None),
            "type": next((upper_map[k] for k in ["CONTENT_TYPE", "MOLECULE_TYPE", "CAT_TYPE"] if k in upper_map), None),
            "species": next((upper_map[k] for k in ["SPECIES", "ORGANISM", "HOST"] if k in upper_map), "Species"),
            "vesicle": next((upper_map[k] for k in ["VESICLE_TYPE", "EV_TYPE", "SUBTYPE"] if k in upper_map), None),
            "method": next((upper_map[k] for k in ["ISOLATION_METHOD", "METHOD"] if k in upper_map), None),
            "year": next((upper_map[k] for k in ["YEAR", "PUBLICATION_YEAR"] if k in upper_map), None)
        }
        return text_cols, col_map
    except:
        return [], {}

SEARCHABLE_COLS, COL_MAP = get_schema_info()

# --- 3. SEARCH ENGINE ---
def search_duckdb(query, limit=100):
    try:
        if not SEARCHABLE_COLS: return []
        
        safe_q = query.replace("'", "''")
        where = " OR ".join([f"\"{c}\" ILIKE '%{safe_q}%'" for c in SEARCHABLE_COLS])
        
        sql = f"SELECT * FROM '{DATA_PATH}' WHERE {where} LIMIT {limit}"
        rows = con.execute(sql).fetchall()
        cols = [d[0] for d in con.description]
        
        output = []
        for r in rows:
            rd = dict(zip(cols, r))
            full_str = str(rd) # For fallback type guessing
            
            # --- Extract & Clean ---
            
            # 1. Name: Try explicit col, then fallback to any ID
            raw_name = rd.get(COL_MAP.get('name'))
            if not raw_name and 'CONTENT_ID' in rd: raw_name = rd['CONTENT_ID'] # Fallback
            name = clean_text(raw_name)
            
            # 2. Type: Standardize casing
            raw_type = rd.get(COL_MAP.get('type'))
            mol_type = standardize_mol_type(raw_type, full_str)
            
            # 3. Others
            species = clean_text(rd.get(COL_MAP.get('species')))
            vesicle = clean_text(rd.get(COL_MAP.get('vesicle')))
            method = clean_text(rd.get(COL_MAP.get('method')))
            year = clean_year(rd.get(COL_MAP.get('year')))

            output.append({
                "name": name,
                "type": mol_type,
                "species": species,
                "vesicle": vesicle,
                "method": method,
                "year": year
            })
            
        return output
    except Exception as e:
        print(e)
        return []

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html", app_name=APP_NAME)

@app.route("/stats")
def stats():
    try:
        c = con.execute(f"SELECT COUNT(*) FROM '{DATA_PATH}'").fetchone()[0]
    except: c = "700k+"
    return jsonify({"total_records": c})

@app.route("/search")
def search():
    q = request.args.get("q", "")
    limit = int(request.args.get("limit", 100))
    if not q: return jsonify([])
    return jsonify(search_duckdb(q, limit))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)