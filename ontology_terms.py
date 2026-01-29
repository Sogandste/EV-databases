# Ontology-aware EV / GO term normalization

EV_TERMS = {
    "exosome": {
        "label": "exosome",
        "ontology_id": "GO:0070062"
    },
    "extracellular vesicle": {
        "label": "extracellular vesicle",
        "ontology_id": "GO:1903561"
    },
    "microvesicle": {
        "label": "microvesicle",
        "ontology_id": "GO:1903561"
    }
}

def normalize_term(term: str):
    if not term:
        return None
    term = term.lower().strip()
    return EV_TERMS.get(term)