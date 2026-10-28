import requests
import pandas as pd

# Import the functions from your existing script
from Priority_Health_Technologies import fetch_all_indicators, find_all_topic_indicators

BASE_URL = "https://ghoapi.azureedge.net/api"


def fetch_indicator_metadata(ind_df, Priority_Health_Technologies_inds):
    # Filter the existing indicator catalog
    metadata_df = ind_df[
        ind_df["IndicatorCode"].isin(
            Priority_Health_Technologies_inds["IndicatorCode"]
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
        "Priority_Health_Technologies_indicator_metadata.csv",
        index=False
    )
    print("Metadata saved successfully!")


if __name__ == "__main__":
    ind_df = fetch_all_indicators()

    Priority_Health_Technologies_inds = find_all_topic_indicators(ind_df)

    metadata_df = fetch_indicator_metadata(
        ind_df, Priority_Health_Technologies_inds
    )

    save_metadata(metadata_df)