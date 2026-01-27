from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)

# --- Mock Data ---
data_ves = [
    {"Gene": "CD63", "Symbol": "CD63", "Organism": "Homo sapiens", "Cargo": "Protein", "Method": "Western Blot", "PMID": "19369651"},
    {"Gene": "CD63", "Symbol": "CD63", "Organism": "Mus musculus", "Cargo": "Protein", "Method": "Mass Spec", "PMID": "21223211"},
    {"Gene": "HSP90", "Symbol": "HSP90AA1", "Organism": "Homo sapiens", "Cargo": "Protein", "Method": "ELISA", "PMID": "19369651"},
]

data_exo = [
    {"Gene": "TSPAN30", "Symbol": "CD63", "Organism": "Homo sapiens", "Cargo": "Protein", "Method": "Flow Cytometry", "PMID": "25678901"},
    {"Gene": "ALIX", "Symbol": "PDCD6IP", "Organism": "Homo sapiens", "Cargo": "Protein", "Method": "Western Blot", "PMID": "22334455"},
]

df_ves = pd.DataFrame(data_ves)
df_ves['Source'] = 'Vesiclepedia'

df_exo = pd.DataFrame(data_exo)
df_exo['Source'] = 'ExoCarta'

df_all = pd.concat([df_ves, df_exo], ignore_index=True)

# --- Harmonization ---
SYNONYM_MAP = {
    "TSPAN30": "CD63",
    "CD63": "CD63",
    "LAMP3": "CD63",
    "ALIX": "PDCD6IP",
    "PDCD6IP": "PDCD6IP"
}

def resolve_synonyms(query):
    q = query.upper().strip()
    return SYNONYM_MAP.get(q, q)

@app.route('/', methods=['GET', 'POST'])
def index():
    results = []
    query = ""
    std = ""
    
    if request.method == 'POST':
        query = request.form.get('search_query', '')
        if query:
            std = resolve_synonyms(query)
            # Filter
            mask = (df_all['Symbol'].str.upper() == std) | (df_all['Gene'].str.upper() == std)
            filtered = df_all[mask]
            if not filtered.empty:
                results = filtered.to_dict('records')
                
    return render_template('index.html', results=results, query=query, std=std)

if __name__ == '__main__':
    app.run(debug=True)