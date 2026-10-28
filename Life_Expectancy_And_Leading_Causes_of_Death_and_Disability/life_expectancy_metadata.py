from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent

for parent in (SCRIPT_DIR, *SCRIPT_DIR.parents):
    if (parent / "metadata_utils.py").is_file():
        sys.path.insert(0, str(parent))
        break
else:
    raise RuntimeError("Could not locate metadata_utils.py.")

from metadata_utils import run_metadata_job

TOPIC = "Life Expectancy and Leading Causes of Death and Disability"

SEARCH_TERMS = ["child deaths",
                    "adolescent mortality rate",
                    "child mortality",
                    "stillbirth rate",
                    "Adult mortality",
                    "Poisoning (unintentional)",
                    "Suicide rates",
                    "NCD deaths",
                    "Dying between the exact ages",
                    "Life expectancy",
                    "Road traffic deaths",
                    "Road traffic death rate"
                    "Homicides",
                    "Maternal deaths",
                    "Maternal mortality"]

OUTPUT_FILE = SCRIPT_DIR / "life_expectancy_indicator_metadata.csv"


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
