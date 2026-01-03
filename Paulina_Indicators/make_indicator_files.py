from pathlib import Path

INDICATOR_CODES = []

TEMPLATE = """\
from pathlib import Path
import pandas as pd
import requests

BASE_URL = "https://ghoapi.azureedge.net/api"
INDICATOR_CODE = "{code}"

OUTDIR = Path(__file__).resolve().parent / "output"
OUTDIR.mkdir(parents=True, exist_ok=True)

def fetch_indicator(code: str) -> pd.DataFrame:
    url = f"{{{{BASE_URL}}}}/Indicator?$filter=IndicatorCode eq '{{{{code}}}}'"
    r = requests.get(url)
    r.raise_for_status()
    return pd.DataFrame(r.json().get("value", []))

def fetch_data(code: str) -> pd.DataFrame:
    url = f"{{{{BASE_URL}}}}/{{{{code}}}}"
    r = requests.get(url)
    r.raise_for_status()
    return pd.DataFrame(r.json().get("value", []))

def to_long(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in ["SpatialDim", "TimeDim", "NumericValue", "Value", "Unit", "Sex", "AgeGroup", "DisplayValue"] if c in df.columns]
    out = df[keep].copy() if keep else df.copy()
    return out

def to_wide(df_long: pd.DataFrame) -> pd.DataFrame:
    if "SpatialDim" in df_long.columns and "TimeDim" in df_long.columns:
        value_col = "NumericValue" if "NumericValue" in df_long.columns else ("Value" if "Value" in df_long.columns else None)
        if value_col:
            return df_long.pivot_table(index="SpatialDim", columns="TimeDim", values=value_col, aggfunc="first").reset_index()
    return df_long

def main():
    df = fetch_data(INDICATOR_CODE)
    df_long = to_long(df)
    df_wide = to_wide(df_long)

    long_path = OUTDIR / f"{{{{INDICATOR_CODE}}}}_long.csv"
    wide_path = OUTDIR / f"{{{{INDICATOR_CODE}}}}_wide.csv"

    df_long.to_csv(long_path, index=False)
    df_wide.to_csv(wide_path, index=False)

    print(f"Saved: {{{{long_path}}}}")
    print(f"Saved: {{{{wide_path}}}}")

if __name__ == "__main__":
    main()
"""

def main():
    base = Path(__file__).resolve().parent
    for code in INDICATOR_CODES:
        folder = base / code
        folder.mkdir(parents=True, exist_ok=True)

        script_path = folder / f"{code}.py"
        script_path.write_text(TEMPLATE.format(code=code), encoding="utf-8")

    print("Created indicator folders + scripts for:")
    for code in INDICATOR_CODES:
        print(" -", code)

if __name__ == "__main__":
    main()
