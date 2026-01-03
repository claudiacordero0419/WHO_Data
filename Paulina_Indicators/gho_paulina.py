import os
import requests
import pandas as pd

BASE_URL = "https://ghoapi.azureedge.net/api"


def fetch_indicator(indicator_code: str) -> pd.DataFrame:
    url = f"{BASE_URL}/{indicator_code}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json().get("value", [])
    return pd.DataFrame(data)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    keep = []
    for c in ["SpatialDim", "TimeDim", "NumericValue", "Value", "Unit", "Dim1", "Dim2", "Dim3"]:
        if c in df.columns:
            keep.append(c)

    out = df[keep].copy()

    if "NumericValue" in out.columns:
        out["value"] = out["NumericValue"]
    elif "Value" in out.columns:
        out["value"] = pd.to_numeric(out["Value"], errors="coerce")
    else:
        out["value"] = pd.NA

    out = out.rename(columns={"SpatialDim": "country", "TimeDim": "year"})
    if "year" in out.columns:
        out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")

    cols = ["country", "year", "value"]
    for extra in ["Unit", "Dim1", "Dim2", "Dim3"]:
        if extra in out.columns:
            cols.append(extra)

    return out[cols].dropna(subset=["country", "year"])


def save_long_and_wide(df_long: pd.DataFrame, out_dir: str, stem: str) -> tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)

    long_path = os.path.join(out_dir, f"{stem}_long.csv")
    wide_path = os.path.join(out_dir, f"{stem}_wide.csv")

    df_long.to_csv(long_path, index=False)

    df_wide = (
        df_long.pivot_table(index="country", columns="year", values="value", aggfunc="first")
        .reset_index()
    )
    df_wide.to_csv(wide_path, index=False)

    return long_path, wide_path


def run_indicator(indicator_code: str, out_dir: str, stem: str) -> tuple[str, str]:
    raw = fetch_indicator(indicator_code)
    tidy = normalize_columns(raw)
    return save_long_and_wide(tidy, out_dir=out_dir, stem=stem)
