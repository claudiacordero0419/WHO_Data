from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent

for parent in (SCRIPT_DIR, *SCRIPT_DIR.parents):
    if (parent / "metadata_utils.py").is_file():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError(
        "error."
    )

from metadata_utils import run_metadata_job

TOPIC = "Health Systems"

SEARCH_TERMS = [
    "density per 100 000 population",
    "median availability of selected generic medicines",
    "median consumer price ratio of selected generic medicines",
    "care-seeking by type of patient and source of care",
    "beds, hospital beds",
]

OUTPUT_FILE = SCRIPT_DIR / "HS_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
