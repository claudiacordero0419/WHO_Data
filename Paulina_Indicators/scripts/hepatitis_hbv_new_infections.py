import requests
import pandas as pd

INDICATOR_CODE = "HEPATITIS_HBV_INFECTIONS_NEW_NUM"

def fetch_indicator(code: str) -> pd.DataFrame:
    url = f"https://ghoapi.azureedge.net/api/{code}"
    resp = requests.get(url)
    resp.raise_for_status()
    rows = resp.json().get("value", [])
    return pd.DataFrame(rows)

def clean_long(df: pd.DataFrame, code: str) -> pd.DataFrame:
    needed_cols = ["SpatialDim", "TimeDim", "NumericValue"]
    missing = [c for c in needed_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing}. Available: {list(df.columns)}")

    out = (
        df[needed_cols]
        .rename(columns={"SpatialDim": "COUNTRY", "TimeDim": "YEAR"})
        .dropna(subset=["COUNTRY", "YEAR"])
    )

    out["YEAR"] = out["YEAR"].astype(int)
    out["IndicatorCode"] = code

    # match your required long format order
    out = out[["COUNTRY", "YEAR", "IndicatorCode", "NumericValue"]]
    return out

def make_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    # Claudia-style wide: index COUNTRY, columns (IndicatorCode, YEAR)
    wide = long_df.pivot_table(
        index="COUNTRY",
        columns=["IndicatorCode", "YEAR"],
        values="NumericValue"
    )
    return wide

def main():
    raw = fetch_indicator(INDICATOR_CODE)
    long_df = clean_long(raw, INDICATOR_CODE)
    wide_df = make_wide(long_df)

    long_path = f"Paulina_Indicators/output/{INDICATOR_CODE}_long.csv"
    wide_path = f"Paulina_Indicators/output/{INDICATOR_CODE}_wide.csv"

    long_df.to_csv(long_path, index=False)
    wide_df.to_csv(wide_path)

    print(f"Saved:\n- {long_path}\n- {wide_path}")
    print(long_df.head())

if __name__ == "__main__":
    main()
