import os
import pyarrow.parquet as pq
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Config ---
APP_NAME = "EVisionary"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

_parquet = None
_columns = []
_total_records = 0

def init_db():
    global _parquet, _columns, _total_records
    try:
        _parquet = pq.ParquetFile(DATA_PATH)
        _columns = _parquet.schema.names
        _total_records = _parquet.metadata.num_rows
        print(f"✅ Loaded {_total_records} records. Columns: {_columns}")
    except Exception as e:
        print(f"❌ Error loading Parquet: {e}")

init_db()

# --- Helper: Safe Value Extractor ---
def get_val(row, potential_keys, default="—"):
    """Try to find a value from a list of potential column names (case-insensitive)."""
    row_keys_lower = {k.lower(): k for k in row.keys()}
    
    for key in potential_keys:
        if key.lower() in row_keys_lower:
            val = row[row_keys_lower[key.lower()]]
            if val: return str(val)
    return default

# --- Logic: Determine Molecule Type ---
def guess_type(row_str, specific_col_val):
    """Guess if it's Protein, RNA, etc. based on content."""
    s = (row_str + " " + specific_col_val).lower()
    if "mirna" in s: return "miRNA"
    if "mrna" in s: return "mRNA"
    if "protein" in s or "uniprot" in s: return "Protein"
    if "lipid" in s: return "Lipid"
    return "Other"

# --- Search Engine (Bulletproof) ---
def universal_search(query, limit=100):
    if not _parquet:
        return []

    query = query.lower().strip()
    results = []
    
    # Read row groups
    for rg in range(_parquet.num_row_groups):
        try:
            # Read ALL columns to ensure we don't miss the keyword
            table = _parquet.read_row_group(rg).to_pylist()
            
            for row in table:
                # 1. Convert ENTIRE row to a single searchable string
                # This guarantees we find the protein if it exists ANYWHERE in the row
                row_values = [str(v) for v in row.values() if v is not None]
                full_row_str = " ".join(row_values).lower()
                
                if query in full_row_str:
                    # 2. Extract Display Data safely
                    
                    # Try to find the Name/Symbol
                    name = get_val(row, ["GENE_SYMBOL", "GENE_NAME", "PROTEIN", "NAME", "SYMBOL", "CONTENT_ID", "ID"])
                    
                    # Try to find Species
                    species = get_val(row, ["SPECIES", "ORGANISM", "HOST"])
                    
                    # Try to find Vesicle Type
                    vesicle = get_val(row, ["VESICLE_TYPE", "EV_TYPE", "SUBTYPE"])
                    
                    # Try to find Method
                    method = get_val(row, ["ISOLATION_METHOD", "METHOD", "TECHNIQUE"])
                    
                    # Try to find Year
                    year = get_val(row, ["YEAR", "DATE", "PUBLICATION_YEAR"])
                    
                    # Guess Type
                    mol_type = get_val(row, ["CONTENT_TYPE", "MOLECULE_TYPE", "TYPE"])
                    if mol_type == "—":
                        mol_type = guess_type(full_row_str, name)

                    results.append({
                        "name": name,
                        "type": mol_type,
                        "species": species,
                        "vesicle": vesicle,
                        "method": method,
                        "year": year
                    })
                    
                    if len(results) >= limit:
                        return results
        except Exception as e:
            print(f"⚠️ Error reading row group {rg}: {e}")
            continue

    return results

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html", app_name=APP_NAME)

@app.route("/stats")
def stats():
    return jsonify({
        "total_records": _total_records,
        "columns": _columns
    })

@app.route("/search")
def search():
    query = request.args.get("q", "")
    limit = int(request.args.get("limit", 100))
    
    if not query: return jsonify([])

    try:
        data = universal_search(query, limit)
        return jsonify(data)
    except Exception as e:
        print(f"Search Error: {e}")
        return jsonify({"error": "Server Error", "details": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)