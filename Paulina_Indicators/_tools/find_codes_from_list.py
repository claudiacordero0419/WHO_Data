import requests
import pandas as pd
import re
from difflib import SequenceMatcher

API = "https://ghoapi.azureedge.net/api/Indicator"

# Put YOUR indicator list here (the human-readable titles you were given)
REQUESTED = [
    "Hepatitis - new infections",
    "Hepatitis - number of persons living with chronic hepatitis infection",
    "Hepatitis - deaths caused by chronic hepatitis infection",
    "Hepatitis - prevalence of chronic hepatitis among general population",
    "Hepatitis - number of chronic hepatitis B-infected persons",
    "Hepatitis - chronic hepatitis treatment rate among diagnosed",
    "Hepatitis - number of persons initiated hepatitis treatment",
    "Hepatitis B surface antigen (HBsAg)",
    "Hepatitis- diagnosis coverage of chronic hepatitis infection",
    "Assistive technology education/training, availability",
    "Assistive technology workforce availability",
    "Assistive technology public funding, availability",
    "Assistive technology government/registered services availability",
    "Assistive technology legislation on access to assistive products",
    "Assistive technology need of assistive products met",
    "Assistive technology use of assistive products",
]

# Map each "requested" item to a SMALL set of keywords that will actually match IndicatorName
KEYWORDS = {
    "Hepatitis - new infections": ["hepatitis", "incidence", "new infections"],
    "Hepatitis - number of persons living with chronic hepatitis infection": ["hepatitis", "chronic", "living", "prevalence"],
    "Hepatitis - deaths caused by chronic hepatitis infection": ["hepatitis", "mortality", "deaths", "chronic"],
    "Hepatitis - prevalence of chronic hepatitis among general population": ["hepatitis", "prevalence", "chronic"],
    "Hepatitis - number of chronic hepatitis B-infected persons": ["hepatitis b", "chronic", "number", "infected"],
    "Hepatitis - chronic hepatitis treatment rate among diagnosed": ["hepatitis", "treatment", "coverage", "diagnosed"],
    "Hepatitis - number of persons initiated hepatitis treatment": ["hepatitis", "initiated", "treatment", "started"],
    "Hepatitis B surface antigen (HBsAg)": ["hbsag", "hepatitis b surface antigen"],
    "Hepatitis- diagnosis coverage of chronic hepatitis infection": ["hepatitis", "diagnosis", "coverage", "diagnosed"],
    "Assistive technology education/training, availability": ["assistive", "technology", "training", "education"],
    "Assistive technology workforce availability": ["assistive", "technology", "workforce"],
    "Assistive technology public funding, availability": ["assistive", "technology", "public funding"],
    "Assistive technology government/registered services availability": ["assistive", "technology", "services", "government"],
    "Assistive technology legislation on access to assistive products": ["assistive", "products", "legislation", "access"],
    "Assistive technology need of assistive products met": ["assistive", "products", "need", "met"],
    "Assistive technology use of assistive products": ["assistive", "products", "use"],
}

def fetch_all_indicators():
    all_rows = []
    url = API
    while url:
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        all_rows.extend(js.get("value", []))
        url = js.get("@data.nextLink")
    return pd.DataFrame(all_rows)

def normalize(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def score_match(requested: str, indicator_name: str) -> float:
    # basic fuzzy similarity on normalized text
    a = normalize(requested)
    b = normalize(indicator_name)
    return SequenceMatcher(None, a, b).ratio()

def find_candidates(ind_df: pd.DataFrame, requested: str, top_k: int = 8):
    kws = KEYWORDS.get(requested, [requested])
    # must match at least one keyword
    mask = False
    name_series = ind_df["IndicatorName"].fillna("")
    for kw in kws:
        mask = mask | name_series.str.contains(kw, case=False, na=False)

    candidates = ind_df.loc[mask, ["IndicatorCode", "IndicatorName"]].drop_duplicates().copy()
    if candidates.empty:
        return candidates

    candidates["similarity"] = candidates["IndicatorName"].apply(lambda x: score_match(requested, x))
    return candidates.sort_values("similarity", ascending=False).head(top_k)

def main():
    ind_df = fetch_all_indicators()

    rows = []
    for requested in REQUESTED:
        cands = find_candidates(ind_df, requested, top_k=8)
        if cands.empty:
            rows.append({"RequestedIndicator": requested, "Rank": "", "IndicatorCode": "", "IndicatorName": "NO MATCH FOUND", "Similarity": ""})
        else:
            for rank, (_, r) in enumerate(cands.iterrows(), start=1):
                rows.append({
                    "RequestedIndicator": requested,
                    "Rank": rank,
                    "IndicatorCode": r["IndicatorCode"],
                    "IndicatorName": r["IndicatorName"],
                    "Similarity": round(float(r["similarity"]), 3),
                })

    out = pd.DataFrame(rows)
    out_path = "Paulina_Indicators/_tools/indicator_code_matches.csv"
    out.to_csv(out_path, index=False)
    print(f"Saved {out_path}")
    print(out.head(30))

if __name__ == "__main__":
    main()
