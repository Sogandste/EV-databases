import os
import pyarrow.parquet as pq
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Config ---
APP_NAME = "EVisionary"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

# --- Global Schema Info (Low RAM) ---
_parquet = None
_columns = []
_total_records = 0
_col_map = {}

def init_db():
    global _parquet, _columns, _total_records, _col_map
    try:
        _parquet = pq.ParquetFile(DATA_PATH)
        _columns = _parquet.schema.names
        _total_records = _parquet.metadata.num_rows
        
        # --- REVIEWER LOGIC: Intelligent Column Mapping ---
        # We try to map the random column names to scientific terms
        # Priorities for Content Type (Protein vs RNA)
        ctype_candidates = ["CONTENT_TYPE", "MOLECULE_TYPE", "TYPE", "CAT_TYPE"]
        # Priorities for Gene/Protein Name
        name_candidates = ["GENE_SYMBOL", "GENE_NAME", "PROTEIN_NAME", "NAME", "SYMBOL", "miRNA_ID"]
        
        _col_map = {
            "type": next((c for c in ctype_candidates if c in _columns), "VESICLE_TYPE"), # Fallback
            "name": next((c for c in name_candidates if c in _columns), None),
            "species": "species" if "species" in _columns else "SPECIES",
            "method": "isolation_method" if "isolation_method" in _columns else "METHOD",
            "year": "YEAR" if "YEAR" in _columns else "year"
        }
        print(f"✅ Schema Mapped: {_col_map}")
        
    except Exception as e:
        print(f"❌ Error loading Parquet: {e}")

init_db()

# --- Search Engine (Streaming) ---
def search_engine(query, limit=100):
    if not _parquet:
        return []

    query = query.lower().strip()
    results = []
    
    # Only read columns necessary for filtering & display (Saves RAM)
    read_cols = list(set([v for k, v in _col_map.items() if v in _columns]))
    
    # Add VESICLE_TYPE strictly if not already there (Context is needed)
    if "VESICLE_TYPE" in _columns and "VESICLE_TYPE" not in read_cols:
        read_cols.append("VESICLE_TYPE")

    for rg in range(_parquet.num_row_groups):
        table = _parquet.read_row_group(rg, columns=read_cols).to_pylist()
        
        for row in table:
            # Construct the searchable text blob
            # We specifically look for matches in the 'name' column first for accuracy
            name_val = str(row.get(_col_map["name"], "")).lower()
            
            # Match Logic:
            # 1. Direct match in Gene/Protein Name (High priority)
            # 2. Match in Species or any other field
            is_match = False
            
            if query in name_val: 
                is_match = True
            elif query in str(row.get(_col_map["species"], "")).lower():
                is_match = True
            elif query in str(row.get("VESICLE_TYPE", "")).lower():
                is_match = True
            elif query in str(row.get(_col_map["type"], "")).lower(): # e.g. searching for "miRNA"
                is_match = True

            if is_match:
                # Format for UI
                results.append({
                    "symbol": row.get(_col_map["name"], "N/A"),
                    "type": row.get(_col_map["type"], "Unknown"), # e.g. Protein, mRNA
                    "species": row.get(_col_map["species"], "Unknown"),
                    "vesicle": row.get("VESICLE_TYPE", "EV"),
                    "method": row.get(_col_map["method"], "N/A"),
                    "year": row.get(_col_map["year"], "-")
                })
                
                if len(results) >= limit:
                    return results

    return results

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html", app_name=APP_NAME)

@app.route("/stats")
def stats():
    return jsonify({
        "total_records": _total_records,
        "mapped_columns": _col_map
    })

@app.route("/search")
def search():
    query = request.args.get("q", "")
    limit = int(request.args.get("limit", 100))
    if not query: return jsonify([])
    try:
        return jsonify(search_engine(query, limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)