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

TOPIC = "Vaccine Preventable Diseases"

SEARCH_TERMS = [
    "vaccine",
    "vaccination",
    "immunization",
    "immunisation",
    "immunization coverage",
    "immunisation coverage",
    "vaccine-preventable disease",
    "vaccine preventable diseases",
    "DTP3",
    "DTP1",
    "diphtheria",
    "tetanus",
    "pertussis",
    "measles",
    "polio",
    "poliomyelitis",
    "rubella",
    "mumps",
    "hepatitis B",
    "Haemophilus influenzae type b",
    "Hib",
    "pneumococcal conjugate vaccine",
    "rotavirus vaccine",
    "HPV vaccine",
    "human papillomavirus",
    "yellow fever vaccine",
    "Japanese encephalitis vaccine",
    "meningococcal vaccine",
    "BCG",
    "zero-dose children",
    "missed communities",
    "national immunization schedule",
    "vaccine introduction",
    'Vaccine-preventable', 'vaccine preventable', 'measles', 'polio', 'diphtheria', 'tetanus', 'pertussis', 'rubella', 'mumps', 'yellow fever', 'rotavirus', 'BCG', 'HPV'
]

OUTPUT_FILE = (
    SCRIPT_DIR / "Vaccine_Preventable_Diseases_indicator_metadata.csv"
)


def main():
    run_metadata_job(TOPIC, SEARCH_TERMS, OUTPUT_FILE)


if __name__ == "__main__":
    main()
