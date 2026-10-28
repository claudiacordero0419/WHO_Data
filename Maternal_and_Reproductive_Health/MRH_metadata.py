import requests
import pandas as pd

# Import the functions from your existing script
from MRH import fetch_all_indicators, find_MRH_indicators

BASE_URL = "https://ghoapi.azureedge.net/api"


def fetch_indicator_metadata(ind_df, MRH_inds):
    # Filter the existing indicator catalog
    metadata_df = ind_df[
        ind_df["IndicatorCode"].isin(
            MRH_inds["IndicatorCode"]
        )
    ].copy()

    # Remove duplicate indicators
    metadata_df = metadata_df.drop_duplicates(
        subset=["IndicatorCode"]
    )

    # Check which metadata fields are available
    print("Available metadata columns:")
    print(metadata_df.columns.tolist())

    # Preview metadata
    print(metadata_df.head())

    return metadata_df



def save_metadata(metadata_df):
    metadata_df.to_csv(
        "MRH_indicator_metadata.csv",
        index=False
    )
    print("Metadata saved successfully!")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    MRH_inds = find_MRH_indicators(ind_df)

    metadata_df = fetch_indicator_metadata(
        ind_df, MRH_inds
    )

    save_metadata(metadata_df)