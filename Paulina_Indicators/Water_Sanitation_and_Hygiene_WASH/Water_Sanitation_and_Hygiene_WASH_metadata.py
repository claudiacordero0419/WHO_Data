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

TOPIC = "Water Sanitation and Hygiene"

SEARCH_TERMS = [
    "water, sanitation and hygiene",
    "water sanitation and hygiene",
    "drinking-water",
    "drinking water",
    "safely managed drinking water",
    "basic drinking water",
    "sanitation",
    "safely managed sanitation",
    "basic sanitation",
    "open defecation",
    "hand hygiene",
    "handwashing",
    "hand washing",
    "hygiene",
    "water quality",
    "water-related disease",
    "WASH",
    "wastewater",
    "waste water",
    "sanitation services",
    "water services",
    "household water treatment",
    "health care facilities water",
    "health care facilities sanitation",
    "healthcare facilities hygiene",
    "water and sanitation in health care facilities",
    "menstrual health",
    "menstrual hygiene",
    "environmental health",'WASH', 'water', 'sanitation', 'hygiene', 'handwashing', 'open defecation', 'diarrhoea', 'diarrhea', 'unsafe WASH'
]

OUTPUT_FILE = (
    SCRIPT_DIR / "Water_Sanitation_and_Hygiene_WASH_indicator_metadata.csv"
)


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
