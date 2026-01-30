import os
import duckdb
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

# --- Config ---
APP_NAME = "EVisionary"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(BASE_DIR, "data", "unified_ev_metadata.parquet")

# --- Initialize DuckDB Connection ---
# We use an in-memory connection that queries the file directly on disk
con = duckdb.connect(database=':memory:')

# --- Dynamic Schema Detection ---
def get_schema_info():
    """
    Analyzes the Parquet file ONCE to find:
    1. Searchable text columns (VARCHAR)
    2. Which column maps to 'Gene', 'Species', etc.
    """
    try:
        # Get column details
        schema_query = f"DESCRIBE SELECT * FROM '{DATA_PATH}' LIMIT 1"
        schema_df = con.execute(schema_query).fetchall()
        
        # columns: [(name, type, ...), ...]
        all_cols = [row[0] for row in schema_df]
        text_cols = [row[0] for row in schema_df if 'VARCHAR' in row[1] or 'STRING' in row[1]]
        
        # Heuristic Mapping for UI Display
        col_map = {
            "name": next((c for c in all_cols if c.upper() in ["GENE_SYMBOL", "GENE", "PROTEIN", "NAME", "SYMBOL", "ID", "CONTENT_ID"]), all_cols[0]),
            "species": next((c for c in all_cols if "SPECIES" in c.upper() or "ORGANISM" in c.upper()), "Species"),
            "vesicle": next((c for c in all_cols if "VESICLE" in c.upper() or "TYPE" in c.upper()), "Vesicle_Type"),
            "method": next((c for c in all_cols if "METHOD" in c.upper() or "ISOLATION" in c.upper()), "Method"),
            "year": next((c for c in all_cols if "YEAR" in c.upper() or "DATE" in c.upper()), "Year"),
            "type": next((c for c in all_cols if c.upper() in ["CONTENT_TYPE", "MOLECULE_TYPE", "CAT_TYPE"]), None)
        }
        
        return text_cols, col_map, len(all_cols)
        
    except Exception as e:
        print(f"❌ Schema Error: {e}")
        return [], {}, 0

# Init schema
SEARCHABLE_COLS, COL_MAP, TOTAL_COLS = get_schema_info()

# --- Search Engine (DuckDB SQL) ---
def search_duckdb(query, limit=100):
    try:
        if not SEARCHABLE_COLS:
            return []
            
        # Clean query to prevent SQL injection issues (basic)
        safe_query = query.replace("'", "''")
        
        # Construct dynamic SQL WHERE clause
        # WHERE col1 ILIKE '%query%' OR col2 ILIKE '%query%' ...
        where_clauses = [f"\"{col}\" ILIKE '%{safe_query}%'" for col in SEARCHABLE_COLS]
        full_where = " OR ".join(where_clauses)
        
        sql = f"""
            SELECT * 
            FROM '{DATA_PATH}' 
            WHERE {full_where} 
            LIMIT {limit}
        """
        
        # Execute Query
        # fetchall() returns a list of tuples
        # We need to map them back to column names
        result_rows = con.execute(sql).fetchall()
        
        # Get result column names (in case SELECT * order changes, though unlikely)
        desc = con.description
        res_col_names = [d[0] for d in desc]
        
        output = []
        for row in result_rows:
            # Create a dict for this row
            row_dict = dict(zip(res_col_names, row))
            
            # Map to standardized UI keys
            # Fallback Logic: If mapped col doesn't exist, use empty string
            mol_type = "Unknown"
            if COL_MAP['type'] and COL_MAP['type'] in row_dict:
                mol_type = row_dict[COL_MAP['type']]
            else:
                # Basic guessing if column missing
                full_str = str(row_dict).lower()
                if "mirna" in full_str: mol_type = "miRNA"
                elif "protein" in full_str: mol_type = "Protein"
                elif "mrna" in full_str: mol_type = "mRNA"

            output.append({
                "name": str(row_dict.get(COL_MAP['name'], "—")),
                "type": str(mol_type),
                "species": str(row_dict.get(COL_MAP['species'], "—")),
                "vesicle": str(row_dict.get(COL_MAP['vesicle'], "—")),
                "method": str(row_dict.get(COL_MAP['method'], "—")),
                "year": str(row_dict.get(COL_MAP['year'], "—"))
            })
            
        return output

    except Exception as e:
        print(f"⚠️ SQL Error: {e}")
        raise e

# --- Routes ---

@app.route("/")
def index():
    return render_template("index.html", app_name=APP_NAME)

@app.route("/stats")
def stats():
    # Fast count using metadata
    try:
        count = con.execute(f"SELECT COUNT(*) FROM '{DATA_PATH}'").fetchone()[0]
    except:
        count = "700k+"
    return jsonify({
        "total_records": count,
        "searchable_columns": len(SEARCHABLE_COLS)
    })

@app.route("/search")
def search():
    query = request.args.get("q", "")
    limit = int(request.args.get("limit", 100))
    
    if not query: return jsonify([])

    try:
        data = search_duckdb(query, limit)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": "Database Error", "details": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)